# dataset_cache.py
"""
High-performance dataset cache for .nc files used by the loader.

Features:
- Disk cache (data/ folder)
- In-memory xarray.Dataset LRU cache with approximate memory accounting
- TTL for cached entries
- Negative cache for missing downloads (404)
- ThreadPool for parallel download/open
- Safe open: passes kwargs to xr.open_dataset correctly (fixes the "takes 1 positional argument but 2 were given" error)
"""

import os
import time
import threading
import requests
import xarray as xr
import numpy as np
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

# DATA_DIR for downloaded files
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


def _approx_dataset_size_bytes(ds):
    """Estimate Dataset memory footprint by summing variable nbytes where available."""
    total = 0
    try:
        # ds.variables is a dict-like of DataArray / Variable objects
        for v in ds.variables:
            try:
                arr = ds.variables[v].values
                if hasattr(arr, "nbytes"):
                    total += int(arr.nbytes)
                else:
                    total += int(np.asarray(arr).size * np.asarray(arr).itemsize)
            except Exception:
                pass
    except Exception:
        total = 0
    return total


class DatasetCache:
    def __init__(self,
                 data_dir=DATA_DIR,
                 max_size_bytes=2 * 1024**3,    # 2 GB default
                 max_items=200,
                 ttl_seconds=3600,              # 1 hour
                 download_workers=4,
                 open_workers=2,
                 retry_downloads=2,
                 session=None):
        self.data_dir = data_dir
        self.max_size_bytes = int(max_size_bytes)
        self.max_items = int(max_items)
        self.ttl_seconds = int(ttl_seconds)
        self.download_workers = int(download_workers)
        self.open_workers = int(open_workers)
        self.retry_downloads = int(retry_downloads)

        self._lock = threading.RLock()
        # OrderedDict: key=url -> (ds, size_bytes, last_used_ts, created_ts)
        self._cache = OrderedDict()
        self._cache_size = 0

        # negative cache: url -> (timestamp, reason)
        self._neg_cache = {}

        # thread pools
        self._dl_pool = ThreadPoolExecutor(max_workers=self.download_workers)
        self._open_pool = ThreadPoolExecutor(max_workers=self.open_workers)

        # requests session
        self._session = session or requests.Session()

    # -------------------------
    # Disk file helpers
    # -------------------------
    def ensure_file(self, url, timeout=30):
        """
        Ensure remote URL is downloaded and return local path.
        If url is already a local path, returns it.
        Raises Exception on permanent failure.
        """
        # if it's a local path, return
        if os.path.exists(url):
            return url

        filename = os.path.join(self.data_dir, url.split("/")[-1])

        # if exists on disk already
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            return filename

        # negative cache check
        with self._lock:
            neg = self._neg_cache.get(url)
            if neg and (time.time() - neg[0]) < self.ttl_seconds:
                raise Exception(f"Previously failed to download {url}: {neg[1]}")

        last_exc = None
        for attempt in range(self.retry_downloads + 1):
            try:
                resp = self._session.get(url, timeout=timeout)
                if resp.status_code != 200:
                    last_exc = Exception(f"Download failed: {url} (status {resp.status_code})")
                    # negative cache for 404/403
                    if resp.status_code in (404, 403):
                        with self._lock:
                            self._neg_cache[url] = (time.time(), f"HTTP {resp.status_code}")
                        raise last_exc
                    time.sleep(0.5 * (attempt + 1))
                    continue

                # write file
                with open(filename, "wb") as fh:
                    fh.write(resp.content)
                return filename
            except Exception as e:
                last_exc = e
                time.sleep(0.5 * (attempt + 1))
                continue

        # permanent failure -> negative cache
        with self._lock:
            self._neg_cache[url] = (time.time(), str(last_exc))
        raise last_exc

    # -------------------------
    # Core: get or open dataset
    # -------------------------
    def get_dataset(self, url, decode_cf=False, mask_and_scale=False, decode_times=False):
        """
        Return an xarray.Dataset for url (or local path).
        Uses in-memory LRU cache and disk cache.
        """
        now = time.time()

        # quick negative cache check
        with self._lock:
            neg = self._neg_cache.get(url)
            if neg and (now - neg[0]) < self.ttl_seconds:
                raise Exception(f"Previously failed to download {url}: {neg[1]}")

            # check in-memory cache
            entry = self._cache.get(url)
            if entry:
                ds, size, last_used, created = entry
                # if TTL expired -> evict
                if (now - created) > self.ttl_seconds:
                    self._evict_key(url)
                else:
                    # update MRU
                    self._cache.move_to_end(url, last=False)
                    self._cache[url] = (ds, size, now, created)
                    return ds

        # not in memory -> ensure file present on disk (may raise)
        local_path = self.ensure_file(url)

        # open dataset in thread pool — pass kwargs properly via helper
        open_kwargs = dict(decode_cf=decode_cf, mask_and_scale=mask_and_scale, decode_times=decode_times)

        def _open_xr(path, kwargs):
            # call xr.open_dataset with kwargs (avoids positional-arg bug)
            return xr.open_dataset(path, **kwargs)

        future = self._open_pool.submit(_open_xr, local_path, open_kwargs)

        try:
            ds = future.result(timeout=60)
        except Exception as e:
            # log for debugging and re-raise so caller skips this file gracefully
            print(f"⚠ Failed to open dataset {local_path}: {e}")
            raise

        # estimate size
        try:
            size = _approx_dataset_size_bytes(ds)
        except Exception:
            size = 0

        with self._lock:
            # make space if needed
            self._make_room_for(size)
            created_ts = time.time()
            # store in cache as MRU (front)
            self._cache[url] = (ds, int(size), created_ts, created_ts)
            self._cache.move_to_end(url, last=False)
            self._cache_size += int(size)

        return ds

    # -------------------------
    # eviction helpers
    # -------------------------
    def _evict_key(self, key):
        """Evict specific key if present."""
        if key not in self._cache:
            return
        try:
            ds, size, lu, cr = self._cache.pop(key)
            try:
                ds.close()
            except Exception:
                pass
            self._cache_size -= int(size)
        except Exception:
            pass

    def _make_room_for(self, size_needed):
        """
        Evict until enough room or items reduced < max_items.
        Recomputes cache_size after eviction for robustness.
        """
        # keep evicting oldest (tail) while constraints violated
        while (self._cache_size + size_needed > self.max_size_bytes) or (len(self._cache) >= self.max_items):
            try:
                k, (ds, sz, lu, cr) = self._cache.popitem(last=True)  # LRU remove
                try:
                    ds.close()
                except Exception:
                    pass
            except KeyError:
                break

            # recompute size
            new_total = 0
            for _, (dds, dsz, dlu, dcr) in self._cache.items():
                new_total += int(dsz)
            self._cache_size = new_total

    # -------------------------
    # utilities
    # -------------------------
    def clear(self):
        """Clear in-memory cache (close datasets) and negative cache."""
        with self._lock:
            keys = list(self._cache.keys())
            for k in keys:
                try:
                    ds, size, lu, cr = self._cache.pop(k)
                    try:
                        ds.close()
                    except Exception:
                        pass
                except Exception:
                    pass
            self._cache_size = 0
            self._neg_cache.clear()

    def status(self):
        with self._lock:
            return {
                "num_cached": len(self._cache),
                "cache_size_bytes": int(self._cache_size),
                "neg_cache_count": len(self._neg_cache)
            }


# Single shared cache instance — tune these defaults to your machine
CACHE = DatasetCache(
    max_size_bytes = 1 * 1024**3,  # 1 GB default — change if you have more memory
    max_items = 120,
    ttl_seconds = 3600,
    download_workers = 2,
    open_workers = 1,
    retry_downloads = 2
)

# # utils/plots.py
# import matplotlib
# matplotlib.use("Agg")

# import matplotlib.pyplot as plt
# import io

# def plot_profile(df, plot_type="temp"):
#     """
#     Returns a PNG (bytes) for temperature/salinity profile.
#     Handles matplotlib correctly for FastAPI.
#     """

#     fig, ax = plt.subplots(figsize=(5, 7))

#     if plot_type in ("temp", "both"):
#         ax.plot(df["temp"], df["depth"], label="Temperature (°C)")

#     if plot_type in ("sal", "both"):
#         ax.plot(df["sal"], df["depth"], label="Salinity (PSU)")

#     ax.invert_yaxis()  
#     ax.set_xlabel("Value")
#     ax.set_ylabel("Depth (m)")
#     ax.grid(True)
#     ax.legend()

#     buf = io.BytesIO()
#     plt.tight_layout()
#     plt.savefig(buf, format="png")
#     plt.close(fig)

#     buf.seek(0)
#     return buf.getvalue()

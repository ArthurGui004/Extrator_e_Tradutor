from yt_dlp import YoutubeDL
from pathlib import Path

url = "https://www.youtube.com/watch?v=7Vg3WozBypI"

ydl_opts = {
    "ffmpeg_location": r"C:\ffmpeg\bin\ffmpeg.exe",
    "outtmpl": str(Path("yt_downloads") / "%(id)s.%(ext)s"),
    "format": "bestaudio/best",
    "quiet": True,
    "no_warnings": True,
    "postprocessors": [{
        "key": "FFmpegExtractAudio",
        "preferredcodec": "mp3",
        "preferredquality": "192",
    }]}

with YoutubeDL(ydl_opts) as ydl:
    info = ydl.(url, download=False)

print(info)
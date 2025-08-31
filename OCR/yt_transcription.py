from pathlib import Path
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
from yt_dlp import YoutubeDL
from markitdown import MarkItDown

class MarkItDownYouTube:
    def __init__(self, workdir="yt_downloads"):
        self.md = MarkItDown()
        self.workdir = Path(workdir)
        self.workdir.mkdir(exist_ok=True)

    def convert_youtube(self, video_id_or_url):
        """
        Extrai texto de um vídeo do YouTube:
        1. Tenta pegar transcript com youtube-transcript-api
        2. Se não houver, baixa áudio com yt_dlp e transcreve
        """
        # Extrair ID se passou URL completa
        if "youtube.com" in video_id_or_url or "youtu.be" in video_id_or_url:
            if "v=" in video_id_or_url:
                video_id = video_id_or_url.split("v=")[1].split("&")[0]
            else:
                video_id = video_id_or_url.split("/")[-1]
        else:
            video_id = video_id_or_url

        # --- Tentar youtube-transcript-api ---
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id)
            texto = "\n".join([entry["text"] for entry in transcript])
            print("✔ Transcript encontrado via youtube-transcript-api")
            return self.md.convert_text(texto).text_content
        except (NoTranscriptFound, TranscriptsDisabled):
            print("⚠ Nenhum transcript encontrado. Caindo para yt-dlp...")
        except Exception as e:
            print(f"⚠ Erro ao pegar transcript: {e}. Caindo para yt-dlp...")

        # --- Fallback: baixar áudio com yt-dlp ---
        ydl_opts = {
            "ffmpeg_location": r"C:\ffmpeg\bin\ffmpeg.exe",
            "outtmpl": str(self.workdir / "%(id)s.%(ext)s"),
            "format": "bestaudio/best",
            "quiet": True,
            "no_warnings": True,
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
        }

        with YoutubeDL(ydl_opts) as ydl:
            ydl.extract_info(video_id_or_url, download=True)

        audio_file = next(self.workdir.glob("*.mp3"), None)
        if audio_file:
            print(f"✔ Áudio baixado: {audio_file}")
            return self.md.convert(str(audio_file)).text_content

        raise RuntimeError("Não foi possível extrair texto do vídeo.")

md = MarkItDownYouTube()
print(md.convert_youtube(video_id_or_url="https://www.youtube.com/watch?v=7Vg3WozBypI"))
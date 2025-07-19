# This is the complete and final code for: src/video_creation/final_video.py

import multiprocessing
import os
import re
import math  # <-- FIX 1: MATH IS IMPORTED
import tempfile
import textwrap
import threading
import time
from os.path import exists
from pathlib import Path
from typing import Dict, Final, Tuple

import ffmpeg
import translators
from PIL import Image, ImageDraw, ImageFont
from rich.console import Console
from rich.progress import track

from utils import settings
from utils.cleanup import cleanup
from utils.console import print_step, print_substep
from utils.fonts import getheight
from video_creation.background import chop_background  # <-- FIX 2: CHOP_BACKGROUND IS IMPORTED
from utils.thumbnail import create_thumbnail
from utils.videos import save_data

console = Console()


class ProgressFfmpeg(threading.Thread):
    def __init__(self, vid_duration_seconds, progress_update_callback):
        threading.Thread.__init__(self, name="ProgressFfmpeg")
        self.stop_event = threading.Event()
        self.output_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        self.vid_duration_seconds = vid_duration_seconds
        self.progress_update_callback = progress_update_callback

    def run(self):
        while not self.stop_event.is_set():
            latest_progress = self.get_latest_ms_progress()
            if latest_progress is not None:
                completed_percent = latest_progress / self.vid_duration_seconds
                self.progress_update_callback(completed_percent)
            time.sleep(1)

    def get_latest_ms_progress(self):
        lines = self.output_file.readlines()
        if lines:
            for line in lines:
                if "out_time_ms" in line:
                    out_time_ms_str = line.split("=")[1].strip()
                    if out_time_ms_str.isnumeric():
                        return float(out_time_ms_str) / 1000000.0
                    else:
                        return None
        return None

    def stop(self):
        self.stop_event.set()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.stop()


def name_normalize(name: str) -> str:
    name = re.sub(r'[?\\"%*:|<>]', "", name)
    name = re.sub(r"( [w,W]\s?\/\s?[o,O,0])", r" without", name)
    name = re.sub(r"( [w,W]\s?\/)", r" with", name)
    name = re.sub(r"(\d+)\s?\/\s?(\d+)", r"\1 of \2", name)
    name = re.sub(r"(\w+)\s?\/\s?(\w+)", r"\1 or \2", name)
    name = re.sub(r"\/", r"", name)
    lang = settings.config["reddit"]["thread"]["post_lang"]
    if lang:
        print_substep("Translating filename...")
        translated_name = translators.translate_text(name, translator="google", to_language=lang)
        return translated_name
    else:
        return name


def prepare_background(reddit_id: str, W: int, H: int) -> str:
    output_path = f"assets/temp/{reddit_id}/background_noaudio.mp4"
    output = (
        ffmpeg.input(f"assets/temp/{reddit_id}/background.mp4")
        .filter("crop", f"ih*({W}/{H})", "ih")
        .output(
            output_path,
            an=None,
            **{
                "c:v": "h264",
                "b:v": "20M",
                "b:a": "192k",
                "threads": multiprocessing.cpu_count(),
            },
        )
        .overwrite_output()
    )
    try:
        output.run(quiet=True)
    except ffmpeg.Error as e:
        print(e.stderr.decode("utf8"))
        exit(1)
    return output_path


def create_fancy_thumbnail(image, text, text_color, padding, wrap=35):
    print_step(f"Creating fancy thumbnail for: {text}")
    font_title_size = 47
    font = ImageFont.truetype(os.path.join("fonts", "Roboto-Bold.ttf"), font_title_size)
    image_width, image_height = image.size
    lines = textwrap.wrap(text, width=wrap)
    y = ((image_height / 2) - (((getheight(font, text) + (len(lines) * padding) / len(lines)) * len(lines)) / 2) + 30)
    draw = ImageDraw.Draw(image)
    username_font = ImageFont.truetype(os.path.join("fonts", "Roboto-Bold.ttf"), 30)
    draw.text((205, 825), settings.config["settings"]["channel_name"], font=username_font, fill=text_color, align="left")

    if len(lines) == 3:
        lines = textwrap.wrap(text, width=wrap + 10)
        font_title_size = 40
        font = ImageFont.truetype(os.path.join("fonts", "Roboto-Bold.ttf"), font_title_size)
        y = ((image_height / 2) - (((getheight(font, text) + (len(lines) * padding) / len(lines)) * len(lines)) / 2) + 35)
    elif len(lines) == 4:
        lines = textwrap.wrap(text, width=wrap + 10)
        font_title_size = 35
        font = ImageFont.truetype(os.path.join("fonts", "Roboto-Bold.ttf"), font_title_size)
        y = ((image_height / 2) - (((getheight(font, text) + (len(lines) * padding) / len(lines)) * len(lines)) / 2) + 40)
    elif len(lines) > 4:
        lines = textwrap.wrap(text, width=wrap + 10)
        font_title_size = 30
        font = ImageFont.truetype(os.path.join("fonts", "Roboto-Bold.ttf"), font_title_size)
        y = ((image_height / 2) - (((getheight(font, text) + (len(lines) * padding) / len(lines)) * len(lines)) / 2) + 30)

    for line in lines:
        draw.text((120, y), line, font=font, fill=text_color, align="left")
        y += getheight(font, line) + padding
    return image


def merge_background_audio(audio: ffmpeg, reddit_id: str):
    background_audio_volume = settings.config["settings"]["background"]["background_audio_volume"]
    if background_audio_volume == 0:
        return audio
    else:
        bg_audio = ffmpeg.input(f"assets/temp/{reddit_id}/background.mp3").filter("volume", background_audio_volume)
        merged_audio = ffmpeg.filter([audio, bg_audio], "amix", duration="longest")
        return merged_audio


def make_final_video(number_of_clips: int, length: int, reddit_obj: dict, background_config: Dict[str, Tuple]):
    W: Final[int] = int(settings.config["settings"]["resolution_w"])
    H: Final[int] = int(settings.config["settings"]["resolution_h"])
    opacity = settings.config["settings"]["opacity"]
    reddit_id = re.sub(r"[^\w\s-]", "", reddit_obj["thread_id"])
    storymode = settings.config["settings"]["storymode"] # Get storymode setting

    # This line from your original code can stay if needed, though not directly used in the unified logic's core path
    # allowOnlyTTSFolder: bool = (settings.config["settings"]["background"]["enable_extra_audio"] and settings.config["settings"]["background"]["background_audio_volume"] != 0)

    print_step("Creating the final video 🎥")

    # These lists will be populated based on the mode
    valid_audio_clips = []
    valid_durations = []
    valid_comment_indices = [] # Specifically for comment mode

    # ====================================================================================
    # MASTER LOGIC SWITCH 1: GATHERING ASSETS (AUDIO AND IMAGE PATHS)
    # This section decides WHICH audio files and image names to look for.
    # ====================================================================================

    if storymode:
        print_substep("Gathering assets for Story Mode video...")
        # For story mode, number_of_clips is the number of text segments from the post.
        # We look for title.mp3 + postaudio-0.mp3, postaudio-1.mp3, ...
        # And title.png + img0.png, img1.png, ...

        title_audio_path = f"assets/temp/{reddit_id}/mp3/title.mp3"
        if exists(title_audio_path):
            valid_audio_clips.append(ffmpeg.input(title_audio_path))
            valid_durations.append(float(ffmpeg.probe(title_audio_path)["format"]["duration"]))
        else:
            console.log("[bold red]FATAL: Story mode title.mp3 not found. Exiting.[/bold red]")
            exit() 

        # 'number_of_clips' in story mode is the count of post segments generated by voices.py
        for i in range(number_of_clips): 
            story_audio_path = f"assets/temp/{reddit_id}/mp3/postaudio-{i}.mp3"
            story_image_path = f"assets/temp/{reddit_id}/png/img{i}.png" 
            
            if exists(story_audio_path) and exists(story_image_path):
                valid_audio_clips.append(ffmpeg.input(story_audio_path))
                valid_durations.append(float(ffmpeg.probe(story_audio_path)["format"]["duration"]))
            else:
                console.log(f"[yellow]Skipping story segment {i} due to missing audio ('{story_audio_path}') or image ('{story_image_path}').[/yellow]")
    
    else: # Comment Mode (Your existing, corrected logic is adapted here)
        print_substep("Intelligently gathering clips for Comment Mode video...")
        target_comments = settings.config["reddit"]["thread"]["number_of_comments"]
        # In comment mode, 'number_of_clips' is the total fetched from main.py (target + buffer)
        total_fetched_comments = number_of_clips 

        title_audio_path = f"assets/temp/{reddit_id}/mp3/title.mp3"
        if exists(title_audio_path):
            valid_audio_clips.append(ffmpeg.input(title_audio_path))
            valid_durations.append(float(ffmpeg.probe(title_audio_path)["format"]["duration"]))
        else:
            console.log("[bold red]FATAL: Comment mode title.mp3 not found. Exiting.[/bold red]")
            exit()

        for i in range(total_fetched_comments):
            if len(valid_comment_indices) == target_comments:
                # console.log(f"[bold green]Successfully gathered {target_comments} valid comment clips.[/bold green]") # Optional log
                break
            comment_audio_path = f"assets/temp/{reddit_id}/mp3/{i}.mp3"
            comment_image_path = f"assets/temp/{reddit_id}/png/comment_{i}.png"
            if exists(comment_audio_path) and exists(comment_image_path):
                valid_audio_clips.append(ffmpeg.input(comment_audio_path))
                valid_durations.append(float(ffmpeg.probe(comment_audio_path)["format"]["duration"]))
                valid_comment_indices.append(i) # Store the original index of the valid comment
            else:
                # This is expected if comments were skipped by screenshotter or TTS, or just not enough were downloaded.
                # You can uncomment the log if you want to see this for each skipped asset.
                # console.log(f"[yellow]Skipping comment asset {i} (missing files), trying next...[/yellow]")
                pass


        if len(valid_comment_indices) < target_comments: # Only show this warning for comment mode
             console.log(f"[bold yellow]Warning: Could only find {len(valid_comment_indices)} valid comments out of a target of {target_comments}. Proceeding with fewer clips.[/bold yellow]")

    # --- UNIFIED LOGIC CONTINUES HERE FOR BOTH MODES ---
    if not valid_audio_clips or not valid_durations: # If only title audio exists, len is 1. If less, something is wrong.
        console.log("[bold red]FATAL: No valid audio clips (not even title) found for the video. Exiting.[/bold red]")
        exit()

    # -1 because the first element is the title, not a content clip
    num_content_clips = len(valid_audio_clips) - 1 if len(valid_audio_clips) > 0 else 0
    print_substep(f"Found {num_content_clips} valid content clips (excluding title).")
    
    # Recalculate the final video length based on ONLY the valid clips found
    length = math.ceil(sum(valid_durations)) # The 'length' parameter passed in is now overridden
    console.log(f"[bold green]Final video length will be: {length} seconds.[/bold green]")
    
    # Chop the background video to the new, accurate length
    chop_background(background_config, length, reddit_obj) 

    background_clip = ffmpeg.input(prepare_background(reddit_id, W=W, H=H)) 
    
    # Combine all the valid audio clips
    audio_concat = ffmpeg.concat(*valid_audio_clips, a=1, v=0)
    final_audio_path = f"assets/temp/{reddit_id}/audio.mp3"
    ffmpeg.output(audio_concat, final_audio_path, **{"b:a": "192k"}).overwrite_output().run(quiet=True)

    # Prepare the final audio (potentially with background music)
    final_audio_input_for_merge = ffmpeg.input(final_audio_path)
    final_audio_output_for_video = merge_background_audio(final_audio_input_for_merge, reddit_id)

    # Create the title image (common to both modes)
    screenshot_width = int((W * 45) // 100)
    Path(f"assets/temp/{reddit_id}/png").mkdir(parents=True, exist_ok=True)
    title_template_image_pil = Image.open("assets/title_template.png")
    thread_title_text_content = reddit_obj["thread_title"]
    normalized_title_text_content = name_normalize(thread_title_text_content)
    font_color_for_title = "#000000"
    padding_for_title = 5
    title_image_pil_generated = create_fancy_thumbnail(title_template_image_pil, normalized_title_text_content, font_color_for_title, padding_for_title)
    title_image_final_path = f"assets/temp/{reddit_id}/png/title.png"
    title_image_pil_generated.save(title_image_final_path)
    
    # ====================================================================================
    # MASTER LOGIC SWITCH 2: VIDEO ASSEMBLY (OVERLAYING IMAGES)
    # This section decides WHICH images to overlay based on the mode.
    # ====================================================================================
    current_time = 0
    
    # Overlay Title (common to both modes)
    if exists(title_image_final_path) and valid_durations: 
        title_duration = valid_durations[0] # Duration of title.mp3
        title_clip_ffmpeg_input = ffmpeg.input(title_image_final_path)["v"].filter("scale", screenshot_width, -1)
        title_overlay_ffmpeg_effect = title_clip_ffmpeg_input.filter("colorchannelmixer", aa=opacity)
        background_clip = background_clip.overlay(
            title_overlay_ffmpeg_effect,
            enable=f"between(t, 0, {title_duration})",
            x="(main_w-overlay_w)/2",
            y="(main_h-overlay_h)/2",
        )
        current_time += title_duration

    # Overlay Story Parts or Comments
    if storymode:
        print_substep("Assembling video for Story Mode...")
        # num_story_parts is the number of postaudio-X.mp3 files
        num_story_parts = len(valid_audio_clips) - 1 
        for i in range(num_story_parts):
            story_part_duration = valid_durations[i + 1] # Get duration for postaudio-i.mp3
            img_path = f"assets/temp/{reddit_id}/png/img{i}.png" # Path to imgX.png

            if exists(img_path):
                story_part_clip_ffmpeg_input = ffmpeg.input(img_path)["v"].filter("scale", screenshot_width, -1)
                story_part_overlay_ffmpeg_effect = story_part_clip_ffmpeg_input.filter("colorchannelmixer", aa=opacity)
                background_clip = background_clip.overlay(
                    story_part_overlay_ffmpeg_effect,
                    enable=f"between(t, {current_time}, {current_time + story_part_duration})",
                    x="(main_w-overlay_w)/2",
                    y="(main_h-overlay_h)/2",
                )
            current_time += story_part_duration

    else: # Comment Mode (Your existing, corrected logic for comments)
        print_substep("Assembling video for Comment Mode...")
        for i, comment_index in enumerate(valid_comment_indices):
            comment_duration = valid_durations[i + 1] # Index i+1 because durations[0] is the title
            img_path = f"assets/temp/{reddit_id}/png/comment_{comment_index}.png"
            
            if exists(img_path): 
                comment_clip_ffmpeg_input = ffmpeg.input(img_path)["v"].filter("scale", screenshot_width, -1)
                comment_overlay_ffmpeg_effect = comment_clip_ffmpeg_input.filter("colorchannelmixer", aa=opacity)
                background_clip = background_clip.overlay(
                    comment_overlay_ffmpeg_effect,
                    enable=f"between(t, {current_time}, {current_time + comment_duration})",
                    x="(main_w-overlay_w)/2",
                    y="(main_h-overlay_h)/2",
                )
            current_time += comment_duration
            
    # --- The rest of your file (thumbnail creation, rendering, etc.) remains the same ---
    # This section starts from the `title = re.sub(...)` line in your original code.
    # Ensure this part is copied correctly from your working file.

    final_title_text = re.sub(r"[^\w\s-]", "", reddit_obj["thread_title"]) 
    final_idx_text = re.sub(r"[^\w\s-]", "", reddit_obj["thread_id"]) 
    title_thumb_text = reddit_obj["thread_title"] 

    filename = f"{name_normalize(final_title_text)[:251]}"
    subreddit = settings.config["reddit"]["thread"]["subreddit"]

    if not exists(f"./results/{subreddit}"):
        print_substep("The 'results' folder could not be found so it was automatically created.")
        os.makedirs(f"./results/{subreddit}")

    allowOnlyTTSFolder = (settings.config["settings"]["background"]["enable_extra_audio"] and settings.config["settings"]["background"]["background_audio_volume"] != 0) # Moved this variable definition here as it's used below.
    if not exists(f"./results/{subreddit}/OnlyTTS") and allowOnlyTTSFolder:
        print_substep("The 'OnlyTTS' folder could not be found so it was automatically created.")
        os.makedirs(f"./results/{subreddit}/OnlyTTS")

    settingsbackground = settings.config["settings"]["background"]
    if settingsbackground["background_thumbnail"]:
        if not exists(f"./results/{subreddit}/thumbnails"):
            print_substep("The 'results/thumbnails' folder could not be found so it was automatically created.")
            os.makedirs(f"./results/{subreddit}/thumbnails")
        first_image = next((file for file in os.listdir("assets/backgrounds") if file.endswith(".png")), None)
        if first_image is None:
            print_substep("No png files found in assets/backgrounds", "red")
        else:
            font_family, font_size_thumb, font_color_thumb = settingsbackground["background_thumbnail_font_family"], settingsbackground["background_thumbnail_font_size"], settingsbackground["background_thumbnail_font_color"] # Renamed font_size and font_color to avoid conflict
            thumbnail = Image.open(f"assets/backgrounds/{first_image}")
            width, height = thumbnail.size
            thumbnailSave = create_thumbnail(thumbnail, font_family, font_size_thumb, font_color_thumb, width, height, title_thumb_text)
            thumbnailSave.save(f"./assets/temp/{reddit_id}/thumbnail.png")
            print_substep(f"Thumbnail - Building Thumbnail in assets/temp/{reddit_id}/thumbnail.png")

    text = f"Background by {background_config['video'][2]}" 
    background_clip = ffmpeg.drawtext(background_clip, text=text, x=f"(w-text_w)", y=f"(h-text_h)", fontsize=5, fontcolor="White", fontfile=os.path.join("fonts", "Roboto-Regular.ttf"))
    background_clip = background_clip.filter("scale", W, H)
    print_step("Rendering the video 🎥")
    from tqdm import tqdm # Keep this import here if it's only used here
    pbar = tqdm(total=100, desc="Progress: ", bar_format="{l_bar}{bar}", unit=" %")

    def on_update_example(progress_value) -> None: # Renamed parameter
        status = round(progress_value * 100, 2)
        old_percentage = pbar.n
        pbar.update(status - old_percentage)

    defaultPath = f"results/{subreddit}"
    with ProgressFfmpeg(length, on_update_example) as progress_ffmpeg_instance: 
        path = defaultPath + f"/{filename}"
        path = path[:251] + ".mp4"
        try:
            ffmpeg.output(background_clip, final_audio_output_for_video, path, f="mp4", **{"c:v": "h264", "b:v": "20M", "b:a": "192k", "threads": multiprocessing.cpu_count()}).overwrite_output().global_args("-progress", progress_ffmpeg_instance.output_file.name).run(quiet=True, overwrite_output=True, capture_stdout=False, capture_stderr=False)
        except ffmpeg.Error as e:
            print(e.stderr.decode("utf8"))
            exit(1)

    old_percentage = pbar.n
    pbar.update(100 - old_percentage)
    if allowOnlyTTSFolder:
        path = defaultPath + f"/OnlyTTS/{filename}"
        path = path[:251] + ".mp4"
        print_step("Rendering the Only TTS Video 🎥")
        with ProgressFfmpeg(length, on_update_example) as progress_ffmpeg_tts_instance: 
            try:
                ffmpeg.output(background_clip, final_audio_input_for_merge, path, f="mp4", **{"c:v": "h264", "b:v": "20M", "b:a": "192k", "threads": multiprocessing.cpu_count()}).overwrite_output().global_args("-progress", progress_ffmpeg_tts_instance.output_file.name).run(quiet=True, overwrite_output=True, capture_stdout=False, capture_stderr=False) 
            except ffmpeg.Error as e:
                print(e.stderr.decode("utf8"))
                exit(1)
        old_percentage = pbar.n
        pbar.update(100 - old_percentage)
    pbar.close()
    save_data(subreddit, filename + ".mp4", final_title_text, final_idx_text, background_config['video'][2])
    print_step("Removing temporary files 🗑")
    cleanups = cleanup(reddit_id)
    print_substep(f"Removed {cleanups} temporary files 🗑")
    print_step("Done! 🎉 The video is in the results folder 📁")

    # ... The rest of your file (thumbnail creation, rendering, etc.) remains the same
    title = re.sub(r"[^\w\s-]", "", reddit_obj["thread_title"])
    idx = re.sub(r"[^\w\s-]", "", reddit_obj["thread_id"])
    title_thumb = reddit_obj["thread_title"]

    filename = f"{name_normalize(title)[:251]}"
    subreddit = settings.config["reddit"]["thread"]["subreddit"]

    if not exists(f"./results/{subreddit}"):
        print_substep("The 'results' folder could not be found so it was automatically created.")
        os.makedirs(f"./results/{subreddit}")

    if not exists(f"./results/{subreddit}/OnlyTTS") and allowOnlyTTSFolder:
        print_substep("The 'OnlyTTS' folder could not be found so it was automatically created.")
        os.makedirs(f"./results/{subreddit}/OnlyTTS")

    settingsbackground = settings.config["settings"]["background"]
    if settingsbackground["background_thumbnail"]:
        if not exists(f"./results/{subreddit}/thumbnails"):
            print_substep("The 'results/thumbnails' folder could not be found so it was automatically created.")
            os.makedirs(f"./results/{subreddit}/thumbnails")
        first_image = next((file for file in os.listdir("assets/backgrounds") if file.endswith(".png")), None)
        if first_image is None:
            print_substep("No png files found in assets/backgrounds", "red")
        else:
            font_family, font_size, font_color = settingsbackground["background_thumbnail_font_family"], settingsbackground["background_thumbnail_font_size"], settingsbackground["background_thumbnail_font_color"]
            thumbnail = Image.open(f"assets/backgrounds/{first_image}")
            width, height = thumbnail.size
            thumbnailSave = create_thumbnail(thumbnail, font_family, font_size, font_color, width, height, title_thumb)
            thumbnailSave.save(f"./assets/temp/{reddit_id}/thumbnail.png")
            print_substep(f"Thumbnail - Building Thumbnail in assets/temp/{reddit_id}/thumbnail.png")

    text = f"Background by {background_config['video'][2]}"
    background_clip = ffmpeg.drawtext(background_clip, text=text, x=f"(w-text_w)", y=f"(h-text_h)", fontsize=5, fontcolor="White", fontfile=os.path.join("fonts", "Roboto-Regular.ttf"))
    background_clip = background_clip.filter("scale", W, H)
    print_step("Rendering the video 🎥")
    from tqdm import tqdm
    pbar = tqdm(total=100, desc="Progress: ", bar_format="{l_bar}{bar}", unit=" %")

    def on_update_example(progress) -> None:
        status = round(progress * 100, 2)
        old_percentage = pbar.n
        pbar.update(status - old_percentage)

    defaultPath = f"results/{subreddit}"
    with ProgressFfmpeg(length, on_update_example) as progress:
        path = defaultPath + f"/{filename}"
        path = path[:251] + ".mp4"
        try:
            ffmpeg.output(background_clip, final_audio_output_for_video, path, f="mp4", **{"c:v": "h264", "b:v": "20M", "b:a": "192k", "threads": multiprocessing.cpu_count()}).overwrite_output().global_args("-progress", progress.output_file.name).run(quiet=True, overwrite_output=True, capture_stdout=False, capture_stderr=False)
        except ffmpeg.Error as e:
            print(e.stderr.decode("utf8"))
            exit(1)

    old_percentage = pbar.n
    pbar.update(100 - old_percentage)
    if allowOnlyTTSFolder:
        path = defaultPath + f"/OnlyTTS/{filename}"
        path = path[:251] + ".mp4"
        print_step("Rendering the Only TTS Video 🎥")
        with ProgressFfmpeg(length, on_update_example) as progress:
            try:
                ffmpeg.output(background_clip, final_audio_input_for_merge, path, f="mp4", **{"c:v": "h264", "b:v": "20M", "b:a": "192k", "threads": multiprocessing.cpu_count()}).overwrite_output().global_args("-progress", progress.output_file.name).run(quiet=True, overwrite_output=True, capture_stdout=False, capture_stderr=False)
            except ffmpeg.Error as e:
                print(e.stderr.decode("utf8"))
                exit(1)
        old_percentage = pbar.n
        pbar.update(100 - old_percentage)
    pbar.close()
    save_data(subreddit, filename + ".mp4", title, idx, background_config["video"][2])
    print_step("Removing temporary files 🗑")
    cleanups = cleanup(reddit_id)
    print_substep(f"Removed {cleanups} temporary files 🗑")
    print_step("Done! 🎉 The video is in the results folder 📁")
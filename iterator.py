import os
import json
import time
import concurrent.futures
import requests
import random
import string
from pathlib import Path
from threading import Lock

class SceneVideoIteratorNode:
    """
    An Iterator Node that:
    1. Sends all scenes to video generation API
    2. Polls API for each video
    3. Downloads videos as they become ready (with parallel downloads)
    4. Waits until all videos are downloaded
    5. Only then proceeds with the workflow
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "scenes_json": ("STRING", {
                    "multiline": True,
                    "default": "[]",
                    "tooltip": "JSON array of scenes with 'scene', 'start', 'end', 'dialogue', and 'scenario' keys"
                }),
                "api_endpoint": ("STRING", {
                    "default": "https://api.example.com/generate-video",
                    "tooltip": "Your video generation API endpoint"
                }),
                "api_key": ("STRING", {
                    "default": "",
                    "tooltip": "API key for authentication"
                }),
                "video_directory": ("STRING", {
                    "default": "output/videos",
                    "tooltip": "Base directory to save downloaded videos"
                }),
                "video_suffix": ("STRING", {
                    "default": "scene_",
                    "tooltip": "Suffix for the video output directory (e.g., 'my_project' creates 'video_my_project')"
                }),
                "enable_random_suffix": ("BOOLEAN", {
                    "default": False,
                    "tooltip": "If enabled, creates a random directory name instead of using video_suffix"
                }),
                "video_extension": (["mp4", "avi", "mov", "mkv"], {
                    "default": "mp4",
                    "tooltip": "File extension for downloaded videos"
                }),
                "poll_interval": ("INT", {
                    "default": 10,
                    "min": 1,
                    "max": 60,
                    "step": 1,
                    "tooltip": "Seconds between API status checks"
                }),
                "timeout": ("INT", {
                    "default": 1800,
                    "min": 0,
                    "max": 7200,
                    "step": 60,
                    "tooltip": "Max wait time in seconds (0 = no timeout)"
                }),
                "max_parallel_downloads": ("INT", {
                    "default": 5,
                    "min": 1,
                    "max": 20,
                    "step": 1,
                    "tooltip": "Maximum number of concurrent downloads"
                }),
                "trigger": ("INT", {
                    "default": 0,
                    "tooltip": "Change this value to re-trigger the node"
                }),
            }
        }

    RETURN_TYPES = ("BOOLEAN", "INT", "STRING", "STRING")
    RETURN_NAMES = ("success", "total_videos", "status_message", "video_directory")
    FUNCTION = "generate_all_videos"
    CATEGORY = "video/generation"
    OUTPUT_NODE = True

    def generate_all_videos(self, scenes_json, api_endpoint, api_key, video_directory,
                           video_suffix, enable_random_suffix, video_extension, poll_interval, 
                           timeout, max_parallel_downloads, trigger):
        """
        Main function that handles the complete workflow.
        """
        try:
            # Parse and validate scenes
            scenes = json.loads(scenes_json)
            total_scenes = len(scenes)

            if total_scenes == 0:
                return (False, 0, "No scenes to process", video_directory)

            # Prepare output directory
            output_dir = self._prepare_output_directory(video_directory, video_suffix, enable_random_suffix)

            print(f"\n{'='*60}")
            print(f"🎬 Starting video generation for {total_scenes} scenes")
            print(f"📁 Output Directory: {output_dir}")
            print(f"{'='*60}\n")

            # Step 1: Submit all scenes to API
            job_ids = self._submit_all_scenes(scenes, api_endpoint, api_key)
            
            if not job_ids:
                return (False, 0, "Failed to submit any scenes to API", output_dir)

            print(f"✅ Submitted {len(job_ids)} scenes to API\n")

            # Step 2: Poll and download videos
            downloaded_count = self._poll_and_download_all(
                scenes, job_ids, api_endpoint, api_key,
                output_dir, video_extension,
                poll_interval, timeout, max_parallel_downloads
            )

            # Step 3: Verify all videos downloaded
            if downloaded_count == total_scenes:
                status = f"✅ SUCCESS! All {total_scenes} videos generated and downloaded"
                print(f"\n{'='*60}")
                print(status)
                print(f"{'='*60}\n")
                return (True, total_scenes, status, output_dir)
            else:
                status = f"⚠️ Only {downloaded_count}/{total_scenes} videos completed"
                print(f"\n{status}\n")
                return (False, downloaded_count, status, output_dir)

        except json.JSONDecodeError as e:
            error_msg = f"❌ JSON parsing error: {e}"
            print(error_msg)
            return (False, 0, error_msg, video_directory)
        except Exception as e:
            error_msg = f"❌ Error: {e}"
            print(error_msg)
            import traceback
            traceback.print_exc()
            return (False, 0, error_msg, video_directory)

    def _prepare_output_directory(self, base_dir, suffix, random_enabled):
        """
        Creates output directory based on suffix and random flag.
        """
        # Ensure base directory exists
        Path(base_dir).mkdir(parents=True, exist_ok=True)
        
        if random_enabled:
            # Generate unique random directory
            while True:
                random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
                dir_name = f"video_{random_suffix}"
                output_dir = os.path.join(base_dir, dir_name)
                if not os.path.exists(output_dir):
                    break
        else:
            # Use provided suffix
            clean_suffix = suffix.strip().replace(" ", "_")
            if not clean_suffix:
                raise ValueError("Please provide a valid suffix or enable random suffix mode.")
            
            dir_name = f"video_{clean_suffix}"
            output_dir = os.path.join(base_dir, dir_name)

            # Check if directory already exists
            if os.path.exists(output_dir):
                raise FileExistsError(
                    f"Directory '{output_dir}' already exists. "
                    f"Please use a different suffix or enable random suffix mode."
                )

        # Create the directory
        os.makedirs(output_dir, exist_ok=True)
        return output_dir

    def _submit_all_scenes(self, scenes, api_endpoint, api_key):
        """
        Submit all scenes to the video generation API in parallel.
        Returns dict of {scene_number: job_id}
        """
        job_ids = {}
        job_ids_lock = Lock()
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        def submit_scene(scene, scene_index):
            """Submit a single scene to API."""
            scene_number = scene.get("scene", scene_index + 1)
            
            # Prepare API request payload
            payload = {
                "prompt": scene.get("scenario", ""),
                "duration": float(scene.get("end", 0)) - float(scene.get("start", 0)),
                "scene_id": scene_number,
                "dialogue": scene.get("dialogue", "")
            }

            try:
                print(f"📤 Submitting scene {scene_number}...")
                
                # API REQUEST - Customize this for your specific API
                response = requests.post(
                    api_endpoint,
                    headers=headers,
                    json=payload,
                    timeout=30
                )

                if response.status_code == 200:
                    result = response.json()
                    # Try different possible job ID field names
                    job_id = result.get("job_id") or result.get("id") or result.get("task_id")
                    
                    if job_id:
                        with job_ids_lock:
                            job_ids[scene_number] = job_id
                        print(f"   ✓ Scene {scene_number} queued (Job: {job_id})")
                        return True
                    else:
                        print(f"   ✗ Scene {scene_number} - No job ID in response")
                        return False
                else:
                    print(f"   ✗ Scene {scene_number} - API error: {response.status_code}")
                    return False

            except requests.exceptions.Timeout:
                print(f"   ✗ Scene {scene_number} - Request timeout")
                return False
            except requests.exceptions.RequestException as e:
                print(f"   ✗ Scene {scene_number} - Request failed: {e}")
                return False
            except Exception as e:
                print(f"   ✗ Scene {scene_number} - Unexpected error: {e}")
                return False

        # Submit all scenes in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(submit_scene, scene, idx) 
                for idx, scene in enumerate(scenes)
            ]
            # Wait for all submissions to complete
            concurrent.futures.wait(futures)

        return job_ids

    def _poll_and_download_all(self, scenes, job_ids, api_endpoint, api_key,
                               video_directory, video_extension,
                               poll_interval, timeout, max_parallel_downloads):
        """
        Poll API for all jobs and download videos as they become ready.
        Uses ThreadPoolExecutor to manage parallel downloads.
        """
        downloaded = set()
        downloaded_lock = Lock()
        total_scenes = len(job_ids)
        headers = {"Authorization": f"Bearer {api_key}"}
        start_time = time.time()
        
        print(f"\n📊 Monitoring {total_scenes} video generation jobs...\n")

        # Track active downloads
        active_downloads = {}  # {scene_number: Future}

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_downloads) as executor:
            while len(downloaded) < total_scenes:
                # Check timeout
                if timeout > 0 and (time.time() - start_time) > timeout:
                    print(f"\n⏱️ Timeout reached after {timeout}s")
                    break

                # Process completed downloads
                finished_scenes = [
                    scene_num for scene_num, future in active_downloads.items() 
                    if future.done()
                ]
                
                for scene_num in finished_scenes:
                    future = active_downloads.pop(scene_num)
                    try:
                        if future.result():
                            with downloaded_lock:
                                downloaded.add(scene_num)
                            print(f"✅ [{len(downloaded)}/{total_scenes}] Scene {scene_num} downloaded")
                        else:
                            print(f"⚠️ Scene {scene_num} download failed")
                    except Exception as e:
                        print(f"❌ Error downloading scene {scene_num}: {e}")

                # Check for available download slots
                available_slots = max_parallel_downloads - len(active_downloads)
                
                if available_slots > 0:
                    # Poll jobs that aren't downloaded or downloading
                    for scene_number, job_id in job_ids.items():
                        if scene_number in downloaded or scene_number in active_downloads:
                            continue
                        
                        if available_slots <= 0:
                            break

                        try:
                            # Poll API for job status
                            # CUSTOMIZE: Change this URL pattern for your API
                            status_url = f"{api_endpoint.replace('/generate-video', '')}/status/{job_id}"
                            response = requests.get(status_url, headers=headers, timeout=10)
                            
                            if response.status_code == 200:
                                result = response.json()
                                status = result.get("status", "").lower()
                                
                                # Check if video is ready
                                if status in ("completed", "finished", "done", "success"):
                                    # Try different possible video URL field names
                                    video_url = (
                                        result.get("video_url") or 
                                        result.get("url") or 
                                        result.get("download_url") or
                                        result.get("result_url")
                                    )
                                    
                                    if video_url:
                                        # Submit download to thread pool
                                        filename = f"scene_{scene_number}.{video_extension}"
                                        filepath = os.path.join(video_directory, filename)
                                        
                                        future = executor.submit(
                                            self._download_video, 
                                            video_url, 
                                            filepath, 
                                            headers
                                        )
                                        active_downloads[scene_number] = future
                                        print(f"⬇️ Starting download for scene {scene_number}...")
                                        available_slots -= 1
                                        
                        except requests.exceptions.Timeout:
                            # Timeout during polling, continue to next job
                            pass
                        except requests.exceptions.RequestException:
                            # Network error during polling, continue
                            pass
                        except Exception:
                            # Other errors, continue polling
                            pass

                # Wait before next poll cycle
                if len(downloaded) < total_scenes:
                    time.sleep(poll_interval)

            # Wait for any remaining downloads to complete
            if active_downloads:
                print(f"\n⏳ Waiting for {len(active_downloads)} remaining downloads...")
                concurrent.futures.wait(active_downloads.values())
                
                # Process final downloads
                for scene_num, future in active_downloads.items():
                    try:
                        if future.result():
                            downloaded.add(scene_num)
                            print(f"✅ [{len(downloaded)}/{total_scenes}] Scene {scene_num} downloaded")
                    except Exception as e:
                        print(f"❌ Error with final download for scene {scene_num}: {e}")

        return len(downloaded)

    def _download_video(self, url, filepath, headers):
        """
        Download video from URL to filepath.
        Returns True on success, False on failure.
        """
        try:
            response = requests.get(url, headers=headers, stream=True, timeout=120)
            
            if response.status_code == 200:
                # Write file in chunks
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:  # Filter out keep-alive chunks
                            f.write(chunk)
                
                # Verify file was written and has content
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    return True
                else:
                    print(f"   ⚠️ Downloaded file is empty or missing: {filepath}")
                    return False
            else:
                print(f"   ⚠️ Download failed with status code: {response.status_code}")
                return False

        except requests.exceptions.Timeout:
            print(f"   ⚠️ Download timeout for: {filepath}")
            return False
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️ Download request error: {e}")
            return False
        except IOError as e:
            print(f"   ⚠️ File write error: {e}")
            return False
        except Exception as e:
            print(f"   ⚠️ Unexpected download error: {e}")
            return False

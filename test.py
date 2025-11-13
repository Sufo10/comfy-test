import json
import time
import logging
import requests
from pathlib import Path
import concurrent.futures

class SceneVideoWanIteratorNode:
    """
    Node that:
      1. Sends each scene to a ComfyUI video generation workflow via HTTP.
      2. Polls for completion.
      3. Downloads the result.
      4. Logs all events with timing and error details.
    """

    INPUT_TYPES = {
        "required": {
            "comfy_api_url": ("STRING", {"default": "http://localhost:8188"}),
            "video_output_dir": ("STRING", {"default": "./videos"}),
            "workflow_path": ("STRING", {"default": "./workflow.json"}),
        },
        "optional": {
            "max_workers": ("INT", {"default": 3, "min": 1, "max": 20}),
            "scenes": ("LIST", {"default": []}),
        },
    }

    RETURN_TYPES = ("LIST",)
    RETURN_NAMES = ("results",)
    FUNCTION = "run_scenes"
    CATEGORY = "Video Generation"
    DESCRIPTION = """
Generates videos for each scene using a ComfyUI workflow.
- `comfy_api_url`: URL of the ComfyUI API.
- `video_output_dir`: Directory to save videos.
- `workflow_path`: Path to the workflow JSON.
- `max_workers`: Number of concurrent threads.
- `scenes`: List of scenes, each containing 'scene', 'scenario', etc.
Returns a list of results per scene.
"""

    def __init__(self):
        self.logger = self._setup_logging()

    def _setup_logging(self):
        log_path = Path("logs")
        log_path.mkdir(exist_ok=True)
        logging.basicConfig(
            filename=log_path / "scene_video_iterator.log",
            level=logging.DEBUG,
            format="[%(asctime)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        return logging.getLogger(__name__)

    def _inject_scene_into_workflow(self, workflow, scenario):
        wf_copy = json.loads(json.dumps(workflow))  # deep copy
        for node_id, node_data in wf_copy["nodes"].items():
            if node_data["class_type"] == "TextPrompt":  # node holding prompt
                node_data["inputs"]["text"] = scenario
        return wf_copy

    def _poll_for_completion(self, comfy_api_url, prompt_id, scene_id, poll_interval=3):
        self.logger.debug(f"Scene {scene_id} - Polling for completion.")
        while True:
            time.sleep(poll_interval)
            response = requests.get(f"{comfy_api_url}/history/{prompt_id}")
            if response.status_code != 200:
                self.logger.warning(f"Scene {scene_id} - Polling failed ({response.status_code}) retrying...")
                continue
            data = response.json()
            if prompt_id in data:
                entry = data[prompt_id]
                if entry.get("status") == "completed":
                    return entry
                elif entry.get("status") == "failed":
                    raise RuntimeError(f"Workflow failed for scene {scene_id}")

    def _download_video(self, comfy_api_url, video_output_dir, result_data, scene_id):
        try:
            video_output_dir = Path(video_output_dir)
            video_output_dir.mkdir(parents=True, exist_ok=True)
            outputs = result_data["outputs"]
            for node_id, node_output in outputs.items():
                for file_info in node_output.get("files", []):
                    file_url = f"{comfy_api_url}/view?filename={file_info['filename']}&subfolder={file_info.get('subfolder', '')}&type=output"
                    local_path = video_output_dir / f"scene_{scene_id}.mp4"
                    r = requests.get(file_url)
                    r.raise_for_status()
                    with open(local_path, "wb") as f:
                        f.write(r.content)
                    return local_path
            raise FileNotFoundError("No output files found in workflow result.")
        except Exception as e:
            self.logger.exception(f"Scene {scene_id} - Video download failed: {e}")
            raise

    def _run_scene(self, comfy_api_url, video_output_dir, workflow_data, scene, max_workers=3):
        scene_id = scene["scene"]
        scenario = scene["scenario"]
        start_time = time.time()
        self.logger.info(f"Scene {scene_id} - Starting video generation for scenario: {scenario}")
        try:
            workflow = self._inject_scene_into_workflow(workflow_data, scenario)
            response = requests.post(f"{comfy_api_url}/prompt", json=workflow)
            response.raise_for_status()
            prompt_id = response.json().get("prompt_id")
            if not prompt_id:
                raise ValueError("No prompt_id returned")
            result = self._poll_for_completion(comfy_api_url, prompt_id, scene_id)
            video_path = self._download_video(comfy_api_url, video_output_dir, result, scene_id)
            duration = round(time.time() - start_time, 2)
            self.logger.info(f"Scene {scene_id} - Completed in {duration}s. Saved to {video_path}")
            return {"scene": scene_id, "path": str(video_path), "status": "done"}
        except Exception as e:
            self.logger.exception(f"Scene {scene_id} - Error: {e}")
            return {"scene": scene_id, "error": str(e), "status": "failed"}

    def run_scenes(self, comfy_api_url, video_output_dir, workflow_path, max_workers=3, scenes=None):
        scenes = scenes or []
        workflow_path = Path(workflow_path)
        with open(workflow_path, "r") as f:
            workflow_data = json.load(f)
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self._run_scene, comfy_api_url, video_output_dir, workflow_data, scene, max_workers) for scene in scenes]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
        return results

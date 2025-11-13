import json
from pathlib import Path

class DynamicWorkflowEditorNode:
    """
    Python node that returns workflow JSON for dynamic editing.
    The JS extension reads this JSON and creates widgets dynamically.
    """

    # Node input
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "workflow_path": ("STRING", {"default": ""}),
            }
        }

    # Node output
    RETURN_TYPES = ("WORKFLOW_JSON",)
    RETURN_NAMES = ("workflow",)
    FUNCTION = "execute"
    CATEGORY = "Workflow Tools"
    DESCRIPTION = "Returns workflow JSON for dynamic input editing in ComfyUI."

    def execute(self, workflow_path):
        path = Path(workflow_path)
        if not path.exists():
            raise FileNotFoundError(f"Workflow JSON not found: {workflow_path}")
        with open(path, "r", encoding="utf-8") as f:
            workflow = json.load(f)

        # Return the JSON directly
        return (workflow,)

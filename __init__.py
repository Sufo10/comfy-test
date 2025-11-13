from .whisper import WhisperTranscribeNode
from .iterator import SceneVideoIteratorNode
from .tester import DynamicWorkflowEditorNode

NODE_CLASS_MAPPINGS = {
    "Whisper Transcribe" : WhisperTranscribeNode,
    "Scene Video Iterator" : SceneVideoIteratorNode,
    "DynamicWorkflowEditorNode" : DynamicWorkflowEditorNode
}

NODE_DISPLAY_NAME_MAPPINGS = {
     "Whisper Transcribe" : "Whisper Transcribe", 
     "Scene Video Iterator" : "Scene Video Custom Iterator",
     "DynamicWorkflowEditorNode": "DynamicWorkflowEditorNode"
}

WEB_DIRECTORY = "./web"

__all__ = ['NODE_CLASS_MAPPINGS', 'NODE_DISPLAY_NAME_MAPPINGS', 'WEB_DIRECTORY']
from .whisper import WhisperTranscribeNode
from .iterator import SceneVideoIteratorNode
from .test import SceneVideoWanIteratorNode

NODE_CLASS_MAPPINGS = { 
    "Whisper Transcribe" : WhisperTranscribeNode,
    "Scene Video Iterator" : SceneVideoIteratorNode,
    "Scene Video WAN Iterator" : SceneVideoWanIteratorNode
}

NODE_DISPLAY_NAME_MAPPINGS = {
     "Whisper Transcribe" : "Whisper Transcribe", 
     "Scene Video Iterator" : "Scene Video Custom Iterator",
     "Scene Video WAN Iterator": "Scene Video WAN Iterator"
}


__all__ = ['NODE_CLASS_MAPPINGS', 'NODE_DISPLAY_NAME_MAPPINGS']
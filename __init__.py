from .whisper import WhisperTranscribeNode
from .iterator import SceneVideoIteratorNode
from .test import SceneVideoWanIteratorNode
from .json import JSONArrayPathMultilineConcatenator

NODE_CLASS_MAPPINGS = { 
    "Whisper Transcribe" : WhisperTranscribeNode,
    "Scene Video Iterator" : SceneVideoIteratorNode,
    "Scene Video WAN Iterator" : SceneVideoWanIteratorNode,
    "JSON Array Path Multiline Concatenator" : JSONArrayPathMultilineConcatenator
}

NODE_DISPLAY_NAME_MAPPINGS = {
     "Whisper Transcribe" : "Whisper Transcribe", 
     "Scene Video Iterator" : "Scene Video Custom Iterator",
     "Scene Video WAN Iterator": "Scene Video WAN Iterator",
     "JSON Array Path Multiline Concatenator": "JSON Array Path Multiline Concatenator"
}


__all__ = ['NODE_CLASS_MAPPINGS', 'NODE_DISPLAY_NAME_MAPPINGS']
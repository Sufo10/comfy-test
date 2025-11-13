from .whisper import WhisperTranscribeNode
from .iterator import SceneVideoIteratorNode

NODE_CLASS_MAPPINGS = {
    "Whisper Transcribe" : WhisperTranscribeNode,
    "Scene Video Iterator" : SceneVideoIteratorNode
}

NODE_DISPLAY_NAME_MAPPINGS = {
     "Whisper Transcribe" : "Whisper Transcribe", 
     "Scene Video Iterator" : "Scene Video Custom Iterator"

}
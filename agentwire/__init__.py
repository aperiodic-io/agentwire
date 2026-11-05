"""AgentWire PoC: lossless framed Zstd + optional lossy agent-context reduction."""
from .zstd import Zstd, train_dictionary
from .transport import (BoundedBatcher, DictionaryRegistry, FrameCodec,
                        Message, StreamDecoder)
from .stream import StreamEncoder, StreamSessionDecoder
from .context import (AgentEvent, ContextReducer, ContentAddressedStore,
                      BudgetExceeded, rehydrate)

__all__ = ["Zstd", "train_dictionary", "BoundedBatcher", "DictionaryRegistry",
           "FrameCodec", "Message", "StreamDecoder", "StreamEncoder", "StreamSessionDecoder", "AgentEvent", "ContextReducer",
           "ContentAddressedStore", "BudgetExceeded", "rehydrate"]

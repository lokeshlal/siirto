from typing import Callable, Any
from siirto.base import Base
from siirto.shared.enums import PlugInType


class FullLoadBase(Base):
    """
    Full load base plugin.
    All full load plugins will be derived from this plugin
    To derive this class, you are expected to override the constructor as well
    as the 'execute' method.

    :param notify_on_completion: callback on completion of full load process
    :type notify_on_completion: Callable[[Any], None]
    """

    # plugin type and plugin name
    plugin_type = PlugInType.Full_Load
    plugin_name = None

    def __init(self,
               notify_on_completion: Callable[[Any], None] = None,
               *args,
               **kwargs):
        self.notify_on_completion = notify_on_completion
        super().__init__(*args, **kwargs)

    def execute(self):
        """
        This is the main method to derive when implementing an operator.
        """
        raise NotImplementedError()

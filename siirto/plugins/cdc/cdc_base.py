from siirto.base import Base
from siirto.shared.enums import PlugInType


class CDCBase(Base):
    """
       CDC base plugin.
       All CDC plugins will be derived from this plugin
       To derive this class, you are expected to override the constructor as well
       as the 'execute' method.
       """

    # plugin type and plugin name
    plugin_type = PlugInType.CDC
    plugin_name = None

    def __init(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self):
        """
        This is the main method to derive when implementing an operator.
        """
        raise NotImplementedError()

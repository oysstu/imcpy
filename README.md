# imcpy
Python bindings for [Inter-Module Communication Protocol (IMC)](https://lsts.fe.up.pt/toolchain/imc) used to communicate between modules in the [LSTS toolchain](https://lsts.fe.up.pt/).

## Installation

### Pip install (PyPi)
Precompiled wheels are available from PyPi, compiled against the standard IMC message set (LSTS).
If your project does not use custom IMC messages, these wheels should be sufficient.

```bash
pip install imcpy
```

Check the release notes or pypi for the precompiled architectures and python versions.

### Source install

#### Clone the project recursively
```bash
git clone --recursive git://github.com/oysstu/imcpy.git
```
This includes the pybind11 submodule.

#### Build the library

```bash
python3 setup.py install
```

If you use the system python and only want to install for a single user, you can add --user to the install command without needing administrator rights. On Windows, the Windows SDK must be installed with Visual Studio and the CMake executable must be on the system PATH.

##### (Optional) Use a specific IMC/Dune version
Replace the dune/imc submodules with the target versions

##### (Optional) Only generate bindings for a subset of IMC messages
A config file named whitelist.cfg can be placed in the root folder to
only create bindings for a subset of the IMC messages. This can be necessary when compiling on
embedded systems, as the linker consumes much memory for the full message set.
If an unknown message is parsed, it will be returned as the Message baseclass rather than a specialized message.
Look at minimal_whitelist.cfg for a set of messages that should always be included.

#### Recommendations
- The imcpy library generates stub files for the bindings, meaning that you can have autocomplete and static type checking if your IDE supports them. This can for example be [PyCharm](https://www.jetbrains.com/pycharm/) or [Jedi](https://github.com/davidhalter/jedi)-based editors.

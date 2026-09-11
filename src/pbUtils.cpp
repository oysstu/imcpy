#include <pybind11/detail/common.h>
#include <pybind11/pybind11.h>
namespace py = pybind11;

#include <string_view>
#include <vector>

void bytes_to_vector(const py::bytes& b, std::vector<char>& vec){
    std::string_view sv(b);
    vec.assign(sv.begin(), sv.end());
}

py::str ascii_to_unicode_safe(std::string_view ascii_str){
  // "replace": replaces characters with unicode question mark
  if (PyObject *str_out = PyUnicode_DecodeASCII(ascii_str.data(), ascii_str.length(), "replace")) {
    // Take ownership
    return py::reinterpret_steal<py::str>(str_out);
  } else {
    // Decoding failed, forward exception
    throw py::error_already_set();
  }
}
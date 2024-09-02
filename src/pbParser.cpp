#include <pybind11/pybind11.h>

#include <string_view>

#include <DUNE/IMC/Message.hpp>
#include <DUNE/IMC/Parser.hpp>

namespace py = pybind11;
using namespace DUNE::IMC;
using namespace pybind11::literals;


void pbParser(py::module &m) {
    py::class_<Parser>(m, "Parser")
    .def(py::init<>())
    .def("reset", &Parser::reset, "Reset bytes stored in parser.")
    .def("parse", [](Parser &obj, py::bytes data) {
        auto data_view = std::string_view(data);
        for(int i = 0; i < data_view.size(); i++){
            Message* msg = obj.parse(data_view[i]);
            if (msg != nullptr) {
              return py::make_tuple<py::return_value_policy::take_ownership>(msg, i+1);
            }
        }

      return py::make_tuple<py::return_value_policy::take_ownership>(nullptr, data_view.size());
    }, py::return_value_policy::take_ownership, "data"_a, "Parse data and return message and consumbed bytes if found.");
}


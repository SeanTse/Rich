__global_dict = {}


def init():
    global __global_dict
    __global_dict = {}


def set_value(name, value):
    global __global_dict
    __global_dict[name] = value


def get_value(name, dft_value=None):
    global __global_dict
    try:
        return __global_dict[name]
    except KeyError:
        return dft_value

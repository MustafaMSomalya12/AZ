import ast

def evaluate_str_literal(val):
    try:
        return ast.literal_eval(val)
    except:
        return val

def unpack_val(val):
    try:
        return val[0]
    except:
        return val

def eval_unpack(val):
    return unpack_val(evaluate_str_literal(val))
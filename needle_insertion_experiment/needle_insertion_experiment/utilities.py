def isfloat(s: str):
    try:
        float(s)
        return True
    except ValueError:
        return False
        
# isfloat


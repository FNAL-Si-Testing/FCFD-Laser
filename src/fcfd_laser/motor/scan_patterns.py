# scan_patterns.py

from typing import Callable, Iterable, Tuple, Dict

def xy_serpentine(nX:int, nY:int, nZ:int):
    for iz in range(nZ):
        for iy in range(nY):
            xs = range(nX) if (iy % 2 == 0) else range(nX - 1, -1, -1)
            for ix in xs:
                yield ix, iy, iz

def xy_raster(nX:int, nY:int, nZ:int):
    for iz in range(nZ):
        for iy in range(nY):
            for ix in range(nX):
                yield ix, iy, iz

def xz_serpentine(nX:int, nY:int, nZ:int):
    for iy in range(nY):
        for iz in range(nZ):
            xs = range(nX) if (iz % 2 == 0) else range(nX - 1, -1, -1)
            for ix in xs:
                yield ix, iy, iz

PATTERNS: Dict[str, Callable[[int, int, int], Iterable[Tuple[int, int, int]]]] = {
    "xy_serpentine": xy_serpentine,
    "xy_raster": xy_raster,
    "xz_serpentine": xz_serpentine,
}

def _coord_from_index(ix:int, iy:int, iz:int, 
                                dX:float, dY:float, dZ:float, 
                                home_X:float, home_Y:float, home_Z:float
                                ) -> Tuple[float,float,float]:
    """
    Calculates the absolute coordinates for a given scan index, relative to a home position.
    """
    return home_X + (ix * dX), home_Y + (iy * dY), home_Z + (iz * dZ)
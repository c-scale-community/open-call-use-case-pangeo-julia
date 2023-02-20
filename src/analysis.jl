"""
countvalid(xout, xin)

    Inner function to count the valid time steps in a datacube.
    This function is aimed to be used inside of a mapCube call.
"""
function countvalid(xout, xin)
    xout .= count(!ismissing, xin)
end

"""
countvalid(cube)

    Outer function to count the number of valid time steps in a cube.
"""
countvalid(cube) = mapCube(countvalid, cube;indims=InDims("Time"), outdims=OutDims())


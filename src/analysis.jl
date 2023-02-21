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


"""
rqatrend(xin, xout, thresh)

Compute the RQA trend metric for the non-missing time steps of xin, and save it to xout. 
`thresh` specifies the epsilon threshold of the Recurrence Plot computation
"""
function rqatrend(pix_trend, pix, thresh=2)
    rp = RecurrenceMatrix(collect(skipmissing(pix)), thresh)
    pix_trend .= trend(rp)
end

"""rqatrend(cube)

Compute the RQA trend metric for the datacube `cube` with the epsilon threshold `thresh`.
"""
function rqatrend(cube, thresh=2)
    mapcube(rqatrend, cube, thresh, indims=InDims("Time"), outdims=OutDims()))
end
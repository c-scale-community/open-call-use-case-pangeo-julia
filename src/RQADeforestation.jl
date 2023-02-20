module RQADeforestation
using Dates
using ArchGDAL: ArchGDAL as AG
using Glob
using YAXArrays

export gdalcube

include("auxil.jl")
include("analysis.jl")
end

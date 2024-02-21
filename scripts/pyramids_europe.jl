# This script is a post-processing script to stitch the forest change data together on a european level
using Glob
using Rasters
using NCDatasets
using Extents
using YAXArrays
using NetCDF
using PyramidScheme
using Statistics
using DiskArrays
using DiskArrayTools
println("all packages loaded")
forestpaths = glob("*.nc", "/eodc/private/pangeojulia/forestaggregated/")
year = 2018
for year in 2018:2021
clusterpaths = glob("*$(year)*.nc", "/eodc/private/pangeojulia/rqatrend_EU/forestmasked_thresh_cluster")


foresttiles = [(parse.(Int, match(r"E(\d\d\d)N(\d\d\d)T3", c).captures)...,)=>c for c in clusterpaths]

idx_to_fname = Dict(foresttiles...)


using DiskArrayTools

xranges = extrema(first.(first.(foresttiles)))
yranges = extrema(last.(first.(foresttiles)))
struct ChunkedFillArray{T,N} <: DiskArrays.AbstractDiskArray{T,N}
    v::T
    s::NTuple{N,Int}
    chunksize::NTuple{N,Int}
end
Base.size(x::ChunkedFillArray) = x.s
DiskArrays.readblock!(x::ChunkedFillArray,aout,r::AbstractVector...) = aout .= x.v
DiskArrays.eachchunk(x::ChunkedFillArray) = DiskArrays.GridChunks(x.s,x.chunksize)
idx_to_fname = Dict(foresttiles...)

a=NetCDF.open(foresttiles[1][2],"layer")
size(a)
f = ChunkedFillArray(a[1,1],size(a),size.(DiskArrays.eachchunk(a)[1],1))

allarrs = [haskey(idx_to_fname,(x,y)) ? NetCDF.open(idx_to_fname[(x,y)],"layer") : f for x in 30:3:57, y in 6:3:36]

#a=NetCDF.open(foresttiles[1][2],"layer")

#foresttuples = [(east=parse(Int, t[2:4]), north=parse(Int, t[6:8])) for t in foresttiles]

#rasts = Raster.(forestpaths, lazy=true);
yaxs = Cube.(forestpaths);
ext = Extents.union(yaxs...)

europex = Rasters._mosaic(first.(dims.(yaxs))...)
europey = Rasters._mosaic(last.(dims.(yaxs))...)
diskarray_merged = DiskArrayTools.ConcatDiskArray(allarrs)
yaxeurope = YAXArray((europex, europey), diskarray_merged)
outpath = "/eodc/private/pangeojulia/clusters_allpyramids/clustered_forestchange_europe_$(year)_pyramid_"

@time "Compute pyramids" raspyr, rasaxs = PyramidScheme.getpyramids(mean ∘ skipmissing, yaxeurope; recursive=false, path=outpath, writeall=true)
end

#unique(diskarray_merged[100000:101000,100000:101000])


#zarr =zcreate(eltype(first(yaxs)), length(europex), length(europey), path="/eodc/private/pangeojulia/rqatrend_EU/clustered_mosaic.zarr", chunks=(1000,1000))

#yaxeurope = YAXArray((europex, europey), zarr)

#yaxeurope[extent(first(yaxs))][:,:] .= first(yaxs)[:,:]

#for yax in yax
#    println("Copy yax")
#    @show extent(yax)
#    yaxeurope[extent(yax)].data .= yax.data
#end



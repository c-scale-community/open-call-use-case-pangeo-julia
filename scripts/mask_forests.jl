using YAXArrays
using NetCDF
using Rasters
using Glob
using NCDatasets
using Zarr
using NetCDF

indir = "/eodc/private/pangeojulia/rqatrend_EU/"
clusterpaths = glob("*.zarr", indir)

forestdir = joinpath(indir, "forestmasked_before_cluster")
mkpath(forestdir)

for c in clusterpaths
    tile = match(r"E\d\d\dN\d\d\dT3", c).match
    forestpath = "/eodc/private/pangeojulia/forestaggregated/compressed_forestaggregated_20M_EU_$(tile).nc"

    if !isfile(forestpath)
        println("Skip non existing forest tile:")
        println(c)
        continue
    end
    yax = Cube(c)
    #ras = Raster(Float32.(Cube(c)))

    forest = Cube(forestpath)
    formask = map((x,y) -> x*y, forest, yax)
    outpath = joinpath(forestdir, "forestmasked_before_cluster" * splitext(basename(c))[1] *".nc")
    @show outpath
    savecube(YAXArray(formask), outpath)
    write(outpath, formask, force=true, deflatelevel=3)
end

using DiskArrays: eachchunk
export rechunk_diskarray


function copydata_mask!(xout,xin1, xin2;threaded=true,dims=:)
    xout .= xin .* xin2
end

function mask_diskarray(aout, ain1, mask;max_cache=5e8,optimargs = (;))


    size(aout) == size(ain) || throw(ArgumentError("Input and Output arrays must have the same size"))
    inar = (InputArray(ain1), InputArray(mask))

    outar = create_outwindows(size(aout),ismem=false,chunks=eachchunk(aout))

    f = create_userfunction(copydata_mask!,eltype(aout),is_blockfunction=true,is_mutating=true)

    op = GMDWop((inar,), (outar,), f);

    lr = optimize_loopranges(op,max_cache;optimargs...)

    r = DaggerRunner(op,lr,(aout,),workerthreads=false)

    run(r)
end

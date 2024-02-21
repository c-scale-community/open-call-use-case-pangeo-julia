using ArchGDAL
using RQADeforestation
using YAXArrays
using Glob
using Dates
using ArgParse
using DimensionalData: Between
using Rasters
using MultipleTesting
using NCDatasets
Threads.nthreads()
YAXArrays.YAXDefaults.workdir[] = "/eodc/private/pangeojulia/"

using Distributed
#nw = 8
#addprocs(nw)
@everywhere begin
    using Pkg
    Pkg.activate("/home/ubuntu/RQADeforestation/")
end

@everywhere using ArchGDAL
@everywhere begin
    using YAXArrays
    using RQADeforestation
    using Dates
end
@everywhere using Logging
@everywhere using LoggingExtras
#@everywhere flog = MinLevelLogger(FileLogger("logfile_rqtrend_d066.txt"), Logging.Warn)
#@everywhere Base.global_logger(flog)

function parse_commandline()
    s = ArgParseSettings()

    @add_arg_table! s begin
        "indir"
            help= "Input directory with data that should be clustered"
            required=true
    end

    return parse_args(s)
end


function main()
    parsedargs = parse_commandline()
    indir = parsedargs["indir"]

#    tile = parsedargs["tile"]
#    thresh= parsedargs["thresh"]
#    orbit = parsedargs["orbit"]
#    pol = parsedargs["pol"]
#    year = parsedargs["year"]
    YAXArrays.YAXDefaults.max_cache[]=2e8

    #indir = "/eodc/private/pangeojulia/germany/VH_A"
    files = glob("*.zarr", indir)
    #continent="EU"
    #@show files
    outdir = joinpath(indir, "cluster")
    @show indir
    @show outdir

    
    mkpath(outdir)
    for p in files
        continent = split(p, "_")[2][1:2]
        crs = RQADeforestation.equi7crs[continent]
        @show crs
        filename = joinpath(outdir, splitext(p)[1] * "_cluster")
        if isfile(filename*".nc")
            println("Skip Computation for $p")

            continue
        end
        println("Doing the cluster computation for: $p")

        ras = Cube(p)
        cmasked = map(ras) do x
            if !ismissing(x)
            x > -1.28 ? 0.0f0 : 1.0f0
            else 
                x 
            end
        end
        data = cmasked.data[:,:]
        replace!(data, missing=>0)
        println("Start labeling")
        labeldata = MultipleTesting.label_components(data,trues(3,3))
        #labels = YAXArray(dims(ras),labeldata)
        @time clusterdata = MultipleTesting.maskcluster(data, labeldata, 30)
        replace!(clusterdata, 0=> missing)
        clusters = YAXArray(dims(ras), clusterdata)
        sampling = DimensionalData.Dimensions.LookupArrays.Intervals{DimensionalData.Dimensions.LookupArrays.Start}(DimensionalData.Dimensions.LookupArrays.Start())
        newdims = set.(dims(clusters), (sampling,))
        clustersras = Raster(clusters.data, newdims;crs)

        @show filename
        @show size(clustersras)
        GC.gc()
        write(filename*".nc", clustersras, force=true, deflatelevel=5)
        #savecube(clusters, filename*".nc", overwrite=true, compress=5 )
        GC.gc()
        println("End of processing for $p", now())
    end
end

main()
using ArchGDAL
using RQADeforestation
using YAXArrays
using Glob
using Dates
using ArgParse
Threads.nthreads()
#YAXArrays.YAXDefaults.workdir[] = "/eodc/private/pangeojulia/"
YAXArrays.YAXDefaults.workdir[] = "/home/ubuntu/RQADeforestation/data"

#using Distributed

#addprocs(nw)
#@everywhere begin
    using Pkg
    Pkg.activate("/home/ubuntu/RQADeforestation/")
#end

#@everywhere using ArchGDAL
#@everywhere begin
    using YAXArrays
    using RQADeforestation
#end
#@everywhere using Logging
#@everywhere using LoggingExtras
#@everywhere flog = MinLevelLogger(FileLogger("logfile_rqtrend_d066.txt"), Logging.Warn)
#@everywhere Base.global_logger(flog)


function parse_commandline()
    s = ArgParseSettings()

    @add_arg_table! s begin
        "--thresh"
            help = "Threshold for the recurrence matrix computation"
            default = 3.
        "--pol", "-p"
            help = "Polarisation that should be stacked"
            default = "VH"
        "--year", "-y"
            help = "Year in which the RQA Trend should be detected. 
            We take a buffer of six month before and after the year to end up with two years of data."
            default = 2018
            arg_type = Int      
        "tile"
            help = "Tile that should be processed"
            required = true
        "orbit"
            help= "Orbit number or 'A' 'D' for ascending and descending"
            required=true
    end

    return parse_args(s)
end

function main(parsedargs=parse_commandline())
    @show typeof(parsedargs)
    @show parsedargs

    #parsedargs = parse_commandline()


    tile = parsedargs["tile"]
    thresh= parsedargs["thresh"]
    orbit = parsedargs["orbit"]
    pol = parsedargs["pol"]
    year = parsedargs["year"]
    indir = "/eodc/products/eodc.eu_sentinel1_backscatter/S1_CSAR_IWGRDH/SIG0/"
    filenames = glob("*/*/$(tile)/*$(pol)_$(orbit)*.tif", indir)
    #tilename = last(splitpath(indir))

    println("loading the data:")
    @time cubevh = RQADeforestation.gdalcube(filenames)
    #s =100
    #subcube = cubevh[X=(cubevh.X[1],cubevh.X[s]), Y=(cubevh.Y[1], cubevh.Y[s])]
    tcube = if year == 0
        cubevh
    else
        cubevh[Time=Date(year-1, 7,1)..Date(year+1,6,30)]
    end
    tax = tcube.Time
    @show tax
    @show size(tcube)
    #@time cubevh[:,:,:];
    #@time cubevh[:,:,:];
    println("Doing the RQA computation:")
    YAXArrays.YAXDefaults.max_cache[]=5e8
    path="/home/ubuntu/RQADeforestation/data/$(tile)_rqatrend_$(pol)_$(orbit)_thresh_$(thresh)_year_$(year).zarr"
    @show path
    #redirect_stdio(stdout="stdout"*stdiopath, stderr="stderr" * stdiopath) do
        println("Start of the processing: ",now())
        #@show nw, nt
        @time RQADeforestation.rqatrend(tcube; thresh, path)
        #@time rqatrendvh = RQADeforestation.rqatrend(cubevh,thresh=4, outpath="/eodc/private/pangeojulia/E048N018T3_rqatrend_$(pol)_D066_$(nw)_$(nt)_$t.zarr")
        println("End of the processing: ",now())
    #end
end

main()
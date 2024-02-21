using ArchGDAL
using RQADeforestation
using YAXArrays
using Glob
using Dates
using ArgParse
using DimensionalData: Between

Threads.nthreads()
YAXArrays.YAXDefaults.workdir[] = "/eodc/private/pangeojulia/"

using Distributed
nw = 4
addprocs(nw)
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
        "--thresh"
            help = "Threshold for the recurrence matrix computation"
            default = 3
        "--pol", "-p"
            help = "Polarisation that should be stacked"
            default = "VH"
        "--continent", "-c"
            help = "Continent for which the tiles are searched should be either 'NA' or 'EU' or '*' to search for all tiles."
            default= "*"
        #"tile"
        #    help = "Tile that should be processed"
        #    required = true
        #"orbit"
        #    help= "Orbit number or 'A' 'D' for ascending and descending"
        #    required=true
    end

    return parse_args(s)
end


function main()
    parsedargs = parse_commandline()
    continent = parsedargs["continent"]

#    tile = parsedargs["tile"]
    thresh= parsedargs["thresh"]
#    orbit = parsedargs["orbit"]
#    pol = parsedargs["pol"]
#    year = parsedargs["year"]
    YAXArrays.YAXDefaults.max_cache[]=8e8

    indir = "/eodc/products/eodc.eu_sentinel1_backscatter/S1_CSAR_IWGRDH/SIG0/"
    tiles = glob("*/*$(continent)*/*T3/", indir)
    splitpath.(tiles)
    tilenames = unique([join(splitpath(tile)[end-1:end], "_") for tile in tiles])
    #tiles = ["E048N018T3"]
#=    tilenames = [
        "E048N024T3",
        "E045N021T3", "E048N021T3","E051N021T3",
        "E045N018T3", "E048N018T3","E051N018T3",
        "E045N015T3", "E048N015T3","E051N015T3",
        ]
=#
    emptytilestxt =  open("/eodc/private/pangeojulia/emptytiles_$continent.txt", "w")
    outdir = "/eodc/private/pangeojulia/rqatrend_$(continent)_allyears_separate_orbits/"
    mkpath(outdir)
    tiles = ["E030N006T3", "E030N009T3", "E030N012T3", "E030N015T3", "E033N006T3", "E033N009T3", "E033N012T3", "E033N015T3", "E033N024T3", "E036N006T3", "E036N009T3", "E036N012T3", "E036N015T3", "E036N018T3", "E036N021T3", "E036N024T3", "E036N027T3", "E039N006T3", "E039N009T3", "E039N012T3", "E039N015T3", "E039N018T3", "E039N021T3", "E039N024T3", "E039N027T3", "E039N030T3", "E042N006T3", "E042N009T3", "E042N012T3", "E042N015T3", "E042N018T3", "E042N021T3", "E042N024T3", "E042N027T3", "E042N030T3", "E045N003T3", "E045N006T3", "E045N009T3", "E045N012T3", "E045N015T3", "E045N018T3", "E045N021T3", "E045N027T3", "E045N030T3", "E048N000T3", "E048N003T3", "E048N006T3", "E048N009T3", "E048N012T3", "E048N015T3", "E048N018T3", "E048N021T3", "E048N024T3", "E048N027T3", "E048N030T3", "E048N033T3", "E051N003T3", "E051N006T3", "E051N009T3", "E051N012T3", "E051N015T3", "E051N018T3", "E051N021T3", "E051N024T3", "E051N027T3", "E051N030T3", "E051N033T3", "E051N036T3", "E054N000T3", "E054N003T3", "E054N006T3", "E054N009T3", "E054N012T3", "E054N015T3", "E054N018T3", "E054N021T3", "E054N024T3", "E054N027T3", "E054N030T3", "E054N033T3", "E054N036T3", "E054N039T3", "E057N000T3", "E057N003T3", "E057N006T3", "E057N009T3", "E057N012T3", "E057N015T3", "E057N018T3", "E057N021T3", "E057N024T3", "E057N027T3", "E057N030T3", "E057N033T3", "E057N036T3", "E057N039T3", "E060N000T3", "E060N006T3", "E060N009T3", "E060N012T3", "E060N015T3", "E060N021T3", "E060N024T3", "E060N027T3", "E060N030T3", "E060N033T3", "E060N036T3", "E060N039T3"]

    for tilefolder in tiles
        @show tilefolder
        #tilefolder = last(split(tile, "_"))

        east = parse(Int, tilefolder[2:4])

        @show east
        if east < 30 && continent == "EU"
            println("Skip tile in the atlantic: $tilefolder")
            continue
        end
        for absorbit in ["A"]#, "D"]
            for pol in ["VH"]#, "VV"]

                @show tilefolder
                filenamesV0M2R4 = glob("V0M2R4/*/$(tilefolder)/*$(pol)_$(absorbit)*.tif", indir)
                filenamesV1M1R1 = glob("V1M1R1/*/$(tilefolder)/*$(pol)_$(absorbit)*.tif", indir)
                allfilenames = AbstractString[filenamesV0M2R4..., filenamesV1M1R1...]
                relorbits = unique([split(basename(x), "_")[5][2:end] for x in allfilenames])
    #tilename = last(splitpath(indir))
                println("loading the data:")
                if isempty(allfilenames)
                    println("Tile: $(tilefolder) is empty.")
                    println(emptytilestxt, tilefolder)
                    flush(emptytilestxt)
                    continue
                end
                for orbit in relorbits
                    filenames = allfilenames[findall(contains("_A$(orbit)_"), allfilenames)]
                @time cubevh = RQADeforestation.gdalcube(filenames)
    #s =100
    #subcube = cubevh[X=(cubevh.X[1],cubevh.X[s]), Y=(cubevh.Y[1], cubevh.Y[s])]
                #for y in  [2018,2016,2017,2019,2020,2021]

                    #@show tax
                    #@show size(tcube)
    #@time cubevh[:,:,:];
    #@time cubevh[:,:,:];
                    println("Doing the RQA computation:")
                    path = joinpath(outdir, "$(tilefolder)_rqatrend_$(pol)_$(orbit)_thresh_$(thresh)_allyears_2017_2022")

                    if !ispath(path* ".done")
                        tcube = cubevh[Time=Between(Date(2017, 7,1), Date(2022,7,1))]
        
                        tax = tcube.Time
                        metafile = open(path * ".txt", "w")
                        @show path
                        @show length(tax)
                        #write(file, tax)
                        println(metafile, "Threshold: ", thresh)
                        println(metafile, "Time Axis: ")
                        show(metafile, MIME("text/plain"), length(tax))
                        println(metafile, "Processed Cube")
                        show(metafile, MIME("text/plain"), tcube)
                        flush(metafile)
        #redirect_stdio(stdout="stdout"*stdiopath, stderr="stderr" * stdiopath) do
                        println("Start of the processing: ",now())
            #@show nw, nt
                        s = now()
                        rqa = redirect_stdout(metafile) do
                            @time rq = RQADeforestation.rqatrend(tcube; thresh, path=path * ".zarr", overwrite=true)
                            return rq
                        end
                        t = now()
                        println(metafile, "Processing time of $pol, $orbit, $tilefolder: ",t-s)
                        #fl32 = map(x->ismissing(x) ? x : Float32(x), rqa)
                        #for (k,v) in fl32.properties
                        #    @show k,v
                        #    if typeof(v) == Float16
                        #        @show k
                        #        fl32.properties[k] *= 1f0
                        #    end
                        #end
                        #fl32.properties["_FillValue"] *= 1f0
                        #fl32.properties["TimeAxis"] = tax
                        #fl32.properties["Threshold"] = thresh
                        #savecube(fl32, path * ".nc", compress=5)

                        touch(path * ".done")
                        @everywhere GC.gc()
                    end
                        println("End of the processing of $pol, $orbit, $tilefolder: ",now())
                end
            end
        end
    end
                    #@time rqatrendvh = RQADeforestation.rqatrend(cubevh,thresh=4, outpath="/eodc/private/pangeojulia/E048N018T3_rqatrend_$(pol)_D066_$(nw)_$(nt)_$t.zarr")
        
    #end
end

main()
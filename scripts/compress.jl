using Rasters
using NCDatasets
using ArgParse
using Glob

function parse_commandline()
    s = ArgParseSettings()

    @add_arg_table! s begin

        "--glob", "-g"
            help="Glob for searching files"
            default="*"
        "--compression", "-c"
            help = "Compression level that should be used from 0 (no compression) to 9"
            default=5
            
        "indir"
            help= "Input directory with data that should be clustered"
            required=true
    end

    return parse_args(s)
end

function main()
    parsedargs = parse_commandline()
    indir = parsedargs["indir"]
    g = parsedargs["glob"]
    deflatelevel = parsedargs["compression"]

    files = glob(g, indir)
    @show g 
    @show indir
    @show files

    for p in files
        filename = splitext(p)[1] * "_compressed.nc"
        if isfile(filename)
            println("Skip $p")
            continue
        end
        @show p
        @time ras = Raster(p)
        #@show ras
        @show eltype(ras)
        filename = splitext(p)[1] * "_compressed.nc"
        @time write(filename,ras; force=true, deflatelevel)
        GC.gc()
    end
end

main()

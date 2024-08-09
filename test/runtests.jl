using RQADeforestation
using Test

@testset "RQADeforestation.jl" begin
    #Load the necessary packages
    using RQADeforestation
    using YAXArrays
    using NetCDF
    # loading the data
    significance_threshold = -1 
    rqathreshold = 1
    inputpath = "data/cVeg_Lmon_MPI-ESM1-2-LR_ssp585_r1i1p1f1_gn_201501-210012.nc"
    outputpath = splitext(inputpath)[1] * "_rqatrend.nc"
    c = Cube(inputpath)
    rqa = RQADeforestation.rqatrend(c; thresh=1., path=outputpath)
    @time rqathresh = map(rqa) do x
        if !ismissing(x)
            x > threshold ? zero(Float32) : one(Float32)
        else 
            x
        end
    end
end

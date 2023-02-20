using YAXArrayBase: GDALBand, GDALDataset, get_var_handle
using DiskArrayTools
using DiskArrays


function getdate(x,reg = r"[0-9]{8}T[0-9]{6}", df = dateformat"yyyymmddTHHMMSS")
    m = match(reg,x).match
    date =DateTime(m,df)
end
 
"""
gdalcube(indir, pol)

Load the datasets in `indir` with a polarisation `pol` as a ESDLArray.
We assume, that `indir` is a folder with geotiffs in the same CRS which are mosaicked into timesteps and then stacked as a threedimensional array.

"""
function gdalcube(indir, pol)
    filenames = glob("*$(pol)*.tif", indir)
    gdalcube(filenames)
end

"""
grouptimes(times, timediff=200000)
Group a sorted vector of time stamps into subgroups
where the difference between neighbouring elements are less than `timediff` milliseconds.
This returns the indices of the subgroups as a vector of vectors.
"""
function grouptimes(times, timediff=200000)
   @assert sort(times) == times
   group = [1]
   groups = [group]

   for i in 2:length(times)
      t = times[i]
      period = t - times[group[end]]
      if period.value < timediff
         push!(group, i)
      else
         push!(groups, [i])
         group = groups[end]
      end
   end
   return groups
end

function DiskArrays.readblock!(b::GDALBand, aout, r::AbstractUnitRange...)
   if !isa(aout,Matrix)
      aout2 = similar(aout)
      AG.read(b.filename) do ds
         AG.getband(ds, b.band) do bh
             DiskArrays.readblock!(bh, aout2, r...)
         end
     end
     aout .= aout2
   else   
   AG.read(b.filename) do ds
       AG.getband(ds, b.band) do bh
           DiskArrays.readblock!(bh, aout, r...)
       end
   end
   end
end

function gdalcube(filenames::AbstractVector{<:AbstractString})
    dates = getdate.(filenames)
    # Sort the dates and files by DateTime
    p = sortperm(dates)
    sdates = dates[p]
    sfiles = filenames[p]
    taxis = RangeAxis(:Time, sdates)

    #@show sdates
    # Put the dates which are 200 seconds apart into groups
    #groupinds = grouptimes(sdates, 200000)

   #datasets = AG.readraster.(sfiles)
   onefile = first(sfiles)
   yax1 = GDALDataset(onefile)
   onecube = Cube(onefile)
   @show onecube.axes
   gdb = get_var_handle(yax1,"Gray")

   @assert gdb isa GDALBand
   all_gdbs = map(sfiles) do f
      gd = GDALBand{eltype(gdb)}(f,gdb.band,gdb.size,gdb.attrs,gdb.cs)
   end 
   stacked_gdbs = diskstack(all_gdbs)
   attrs = copy(gdb.attrs)
   #attrs["add_offset"] = Float16(attrs["add_offset"])
   attrs["scale_factor"] = Float16(attrs["scale_factor"])
   all_cfs = CFDiskArray(stacked_gdbs,attrs)
   return YAXArray([onecube.axes...,taxis],all_cfs, onecube.properties)
   #datasetgroups = [datasets[group] for group in groupinds]
    #We have to save the vrts because the usage of nested vrts is not working as a rasterdataset
    #temp = tempdir()
    #outpaths = [joinpath(temp, splitext(basename(sfiles[group][1]))[1] * ".vrt") for group in groupinds]
    #vrt_grouped = AG.unsafe_gdalbuildvrt.(datasetgroups)
    #AG.write.(vrt_grouped, outpaths)
    #vrt_grouped = AG.read.(outpaths)
    #vrt_vv = AG.unsafe_gdalbuildvrt(vrt_grouped, ["-separate"])
    #rvrt_vv = AG.RasterDataset(vrt_vv)
    #yaxras = YAXArray.(sfiles)
    #cube = concatenatecubes(yaxras, taxis)
    #bandnames = AG.GDAL.gdalgetfilelist(vrt_vv.ptr)



    # Set the timesteps from the bandnames as time axis
    #dates_grouped = [sdates[group[begin]] for group in groupinds]
end
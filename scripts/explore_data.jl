using RQADeforestation
using YAXArrays

indir, pol = "/eodc/products/eodc.eu_sentinel1_backscatter/S1_CSAR_IWGRDH/SIG0/V1M1R1/EQUI7_EU020M/E048N018T3","VH"

cubevh = gdalcube(indir, pol)

subcube = cubevh[X=(4.8e6,4.81e6), Y=(2e6,1.99e6)]

valsub = RQADeforestation.countvalid(subcube)
from ftplib import FTP, error_perm
import can_decoder as cd


state = 'ES'
year = 2019
year2 = str(year)[-2:].zfill(2)

if year < 1979:
    raise ValueError("SIM does not contain data before 1979")
elif year >= 1996:
    ftp_dir = "/dissemin/publicos/SIM/CID10/DORES"
    fname = "DO{}{}.dbc".format(state, year)
else:
    ftp_dir = "/dissemin/publicos/SIM/CID9/DORES"
    fname = "DOR{}{}.dbc".format(state, year2)

# cachefile = os.path.join(CACHEPATH, "SIM_" + fname.split(".")[0] + "_.parquet")

ftp = FTP("ftp.datasus.gov.br")
ftp.login()
ftp.cwd(ftp_dir)

try:
    ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)
except error_perm:
    try:
        ftp.retrbinary("RETR {}".format(fname.upper()), open(fname, "wb").write)
    except:
        raise Exception("File {} not available".format(fname))

dbc = cd.load_dbc(fname)
df_decoder = cd.DataFrameDecoder(dbc)

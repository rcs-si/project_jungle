import parsechunk
import df

copy_lines('/projectnb/scv/atime/projectnb_econdept.list',' /projectnb/rcs-intern/alana/objects/parsed.csv')

df = load_data('/projectnb/rcs-intern/alana/objects/parsed.csv')

df.head()


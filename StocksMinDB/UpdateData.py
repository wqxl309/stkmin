
import asyncio
import time
from StocksMinDB.StocksMinDb import StocksMinDB



async def obj_updt(obj,folder,seed):
    print('The {0}th obj:'.format(seed))
    s=time.time()
    await obj.update_data_by_stock(tempfolder=folder,seed=seed)
    print('The {0}th obj finished with {1} seconds'.format(seed,time.time()-s))

def multi_updt(config,cornum,folder):
    print('Update folder {0} with {1} corutines'.format(folder,cornum))
    objs = []
    tasks = []
    for dumi in range(cornum):
        objs.append(StocksMinDB(config,cornum=cornum))
        print(dumi,objs[dumi])
        tasks.append(asyncio.ensure_future(obj_updt(obj=objs[dumi],folder=folder,seed=dumi)))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()

if __name__=='__main__':
    c = r'E:\stocks_data_min\StocksMinDB\configs'
    # obj = StocksMinDB(c)
    # s=time.time()
    # obj.update_data_by_stock('200201-06')
    # print(time.time()-s)

    fld = '200201-06_t'

    # s = time.time()
    # obj = StocksMinDB(c)
    # obj.update_data_by_stock(tempfolder=fld)
    # print(time.time()-s)

    cornum = 5
    s = time.time()
    multi_updt(config=c,cornum=cornum,folder=fld)
    print(time.time()-s)

"""

run as: 

    export OPS_ENV=dev
    .  ~/.bash_profile 
    pushd /opt/_lab/caiso/_src/

    export OPS_ENV=dev ; .  ~/.bash_profile ; pushd /opt/_lab/caiso/_src/


    python etl_0_caiso_files.py -d debug       

NOTE 

conda info --envs
source activate py311



"""



import argparse as ap
import logging as lg
import os
import pandas as pd
import psycopg2 as pg
import sys



###
###  ARGV 
###

prs0 = ap.ArgumentParser( prog='ETL', description='ETL for loading database', epilog='')
prs0.add_argument('-d', '--debug_level'
                  ,help = 'debugging log level' 
                  ,required = False
                  ,default = 'info'
                  ,choices = ['debug', 'info', 'warning', 'error', 'critical']
                  )
args = prs0.parse_args()


###
###  ENV 
###

# NOTE/TODO here is a good place to determine dev/test/prod from the env
# NOTE remember to set the env var OPS_ENV  with 
# 
home0     = os.environ['HOME']
ops_env0  = os.environ['OPS_ENV']

dir_path = os.path.dirname(os.path.realpath(__file__))
uname0   = os.uname()

fil1 = __file__.split('.')
fil2 = fil1[0]
fil3 = fil2 + '.log'

pth1 = dir_path.split('/') 

pth2 = pth1[:-1]
pth2.append('_log')
pth2.append(fil3)
pth3 = '/'.join(pth2)





###
### CONFIG
###

# TODO change file_type from template to ETL, DTP, RPT, VIZ, MON & etc
#log_id = ops_env0  + ' template' 
log_id = ops_env0 + ' ' + __file__

# TODO consider reading this in via a config file.

log0 = lg.getLogger(log_id)
if   args.debug_level == 'debug':    log0.setLevel(lg.DEBUG)
elif args.debug_level == 'info':     log0.setLevel(lg.INFO)
elif args.debug_level == 'warning':  log0.setLevel(lg.WARNING)
elif args.debug_level == 'error':    log0.setLevel(lg.ERROR)
elif args.debug_level == 'critical': log0.setLevel(lg.CRITICAL)
else:                                log0.setLevel(lg.INFO)

fmt0 = lg.Formatter('%(asctime)s : %(name)s - %(levelname)s - %(message)s')

#fil2 = lg.FileHandler('/opt/_lab/iconiq/log/etl_test.log')
fil2 = lg.FileHandler(pth3)
fil2.setFormatter(fmt0)

log0.addHandler(fil2)

log0.debug('debug message')
log0.info('info message')
log0.warning('warn message')
log0.error('error message')
log0.critical('critical message')





log0.debug('')
log0.debug(f'dir_path    {dir_path}')
log0.debug(f'__file__    {__file__}')
log0.debug(f'pth1        {pth1}')
log0.debug(f'pth2        {pth2}')
log0.debug(f'pth3        {pth3}')
log0.debug(f'uname0      {uname0}')
log0.debug(f'home0       {home0}')
log0.debug(f'ops_env0    {ops_env0}')
log0.debug('')


''' 
NOTE - read the ~/.pgpass file for db credentials. Note that cre1 cre2
variables for working with credential strings for the purpose of 
loading class variabes for host, port, database, user, and password.
'''
hst0,prt0,db0,usr0,pas0  = None,None,None,None,None
creds_file = os.path.expanduser('~/.pgpass')
with open(creds_file,'r') as f0:
    f1 = f0.readlines()
cre0 = f1[0]
cre1 = cre0.split(':')
#cre1[2] = 'hf3' if cre1[2] == '*' else cre1[2]
cre1[2] = 'caiso' if cre1[2] == '*' else cre1[2]
cre1[4] = cre1[4].rstrip() 

hst0 = cre1[0]
prt0 = cre1[1]
db0  = cre1[2]
usr0 = cre1[3]
pas0 = cre1[4]


log0.debug('')
log0.debug(f'================================================================')
log0.debug('')
log0.debug(f'hst0        {hst0} ')
log0.debug(f'prt0        {prt0} ')
log0.debug(f'db0         {db0} ')
log0.debug(f'usr0        {usr0} ')
log0.debug(f'pas0        {pas0} ')



###
### GLOBALS
###

sel9 = ''' 
select dw_file
    ,count(*) as c0
    ,min(ts0) as min_ts0
    ,max(ts0) as max_ts0
from core.ems_load_hourly_hist
group by 1
order by min_ts0
''' 

sel0 = ''' 

select *
from hf3.information_schema.tables
where true
    --and table_schema not in ('pg_catalog','information_schema')

'''

del0 = ' delete from stg.pay0'
del10 = ' delete from stg.pay10'
del20 = ' delete from stg.pay20'

sel1 = ''' 
select *
from stg.pay0
where true
'''

sel2 = ''' 
select dt0
    ,count(*) as c0
    ,min(dt0) as min_dt0
    ,max(dt0) as max_dt0
from core.ems_load_hourly_hist
group by 1
order by 1
'''


# TODO do md5 on payload

ins1_bak = f'''
insert into core.tic_hist
(tic,dt0,open,high,low,close,adj_close,volume)
with cte0 as (
select
    split_part(dw_file,'.',1)    as tic0
    ,split_part(payload,',',1)    as dt0
    ,split_part(payload,',',2)    as open0
    ,split_part(payload,',',3)    as high0
    ,split_part(payload,',',4)    as low0
    ,split_part(payload,',',5)    as close0
    ,split_part(payload,',',6)    as adj_close0
    ,split_part(payload,',',7)    as vol0
from stg.tic
)
select
     tic0
    ,dt0::date              as dt0
    ,open0::numeric         as open0
    ,high0::numeric         as high0
    ,low0::numeric          as low0
    ,close0::numeric        as close0
    ,adj_close0::numeric    as adj_close0
    ,vol0::numeric          as vol0
from cte0
where true
    and dt0 not ilike '%date%'
'''



ins1 = f'''
insert into core.ems_load_hourly_hist as ems
(ts0,dt0,he,pge,sce,sdge,vea,caiso_total,pay_sha256,dw_file)
with cte0 as (

select *
    from stg.pay0
where true
    and payload not ilike '%date%'
    and length(payload) > 40


), cte1 as (
select
     to_timestamp(     split_part(payload,',',1)
             || ' ' || split_part(payload,',',2)::int-1
             || ':00' ,'MM/DD/YY HH24:MI')::timestamp
                      without time zone               as ts0

             --at time zone 'PST'    as ts0
             --at time zone 'PST' at time zone 'PST'    as ts0
    ,to_date(split_part(payload,',',1),'MM/DD/YY')    as dt0
    ,split_part(payload,',',2)                        as he
    ,split_part(payload,',',3)                        as pge
    ,split_part(payload,',',4)                        as sce
    ,split_part(payload,',',5)                        as sdge
    ,split_part(payload,',',6)                        as vea
    ,split_part(payload,',',7)                        as caiso_total
    ,sha256(convert_to(payload, 'LATIN1'))            as pay_sha256
    ,dw_file
from cte0

)
select
      ts0
     ,dt0
     ,he::numeric
     ,pge::numeric
     ,sce::numeric
     ,sdge::numeric
     ,vea::numeric
     ,caiso_total::numeric
     ,pay_sha256
     ,dw_file
from cte1
where true
    and pay_sha256 not in (
            select pay_sha256
            from core.ems_load_hourly_hist
            )
'''



ins11 = f'''
insert into core.fuel_mix_1_hr_hist as fmx
(ts0 ,solar ,wind ,geothermal ,biomass ,biogas ,small_hydro
        ,coal ,nuclear ,natural_gas ,large_hydro ,batteries
        ,imports ,other ,pay_sha256,dw_file)

with cte0 as (

select *
from stg.pay10
where true
    and payload not ilike '%interval%'
    and payload not ilike '%,,%'
    and length(payload) > 40
), cte1 as (
select
     split_part(payload,',',1)::timestamp  as ts0
    ,split_part(payload,',',4)::numeric    as solar
    ,split_part(payload,',',5)::numeric    as wind
    ,split_part(payload,',',6)::numeric    as geothermal
    ,split_part(payload,',',7)::numeric    as biomass
    ,split_part(payload,',',8)::numeric    as biogas
    ,split_part(payload,',',9)::numeric    as small_hydro
    ,split_part(payload,',',10)::numeric   as coal
    ,split_part(payload,',',11)::numeric   as nuclear
    ,split_part(payload,',',12)::numeric   as natural_gas
    ,split_part(payload,',',13)::numeric   as large_hydro
    ,split_part(payload,',',14)::numeric   as batteries
    ,split_part(payload,',',15)::numeric   as imports
    ,split_part(payload,',',16)::numeric   as other
    ,sha256(convert_to(payload, 'LATIN1')) as pay_sha256
    ,dw_file
from cte0

)
select *
from cte1
where true
    and pay_sha256 not in (
            select pay_sha256
            from core.fuel_mix_1_hr_hist 
            )
'''

ins21 = f'''
insert into core.load_1_hr_hist as lod
(ts0 ,caiso_load ,pay_sha256 ,dw_file)
with cte0 as (

select *
from stg.pay20
where true
    and payload not ilike '%interval%'
    and payload not ilike '%,,%'
    and length(payload) > 40

), cte1 as (

select
     split_part(payload,',',1)::timestamp  as ts0
    ,split_part(payload,',',4)::numeric    as caiso_load
    ,sha256(convert_to(payload, 'LATIN1')) as pay_sha256
    ,dw_file
from cte0

)
select *
from cte1
where true
    and pay_sha256 not in (
            select pay_sha256
            from core.load_1_hr_hist
            )

'''



class ETLA(object):
    ''' TODO comment.
    '''
    def __init__(self):
        ''' TODO comment.
        '''
        self._hst0 = hst0
        self._prt0 = prt0
        self._db0  = db0
        self._usr0 = usr0
        self._pas0 = pas0  

        self._con0 = None
        self._cur0 = None
        self._df0  = None

        self._files = None
        #self._pth0  = '/opt/_lab/proto/_data/yfi_csv/'
        self._pth0  = '../_dat/'


    def __del__(self):
        ''' TODO comment.
        '''
        self._con0 = None
        self._cur0 = None
        self._df0  = None


    def con_open(self):
        ''' TODO comment. 
        '''
        try: 
            self._con0 = pg.connect(
                     host      = self._hst0 
                    ,port      = self._prt0 
                    ,database  = self._db0  
                    ,user      = self._usr0 
                    ,password  = self._pas0 
                    )
            self._con0.autocommit = True
        except Exception as e1: 
            log0.debug(f'con_open e1 {e1}')


    def con_close(self):
        ''' TODO comment. 
        '''
        try: 
            if self._con0 is not None: 
                if self._cur0 is not None: 
                    self._cur0.close()
                self._con0.close()
        except Exception as e2: 
            log0.debug(f'con_close e2 {e2}')
    

    def sql_exec(self,sql0):
        ''' TODO comment. 
        '''
        if self._con0 is not None: 
            self._cur0 = self._con0.cursor()
            try:
                self._cur0.execute(sql0)
                self._df0 = pd.DataFrame(self._cur0.fetchall())

                cols = [desc[0] for desc in self._cur0.description]
                self._df0.columns = cols

            except Exception as e3:
                log0.debug(f'con_close e3 {e3}')


    def fs_scan(self):
        ''' TODO comment. 
        '''
        self._files = [f0 for f0 in sorted(os.listdir(self._pth0)) 
                if os.path.isfile(os.path.join(self._pth0 , f0))
                ]


    def print_df0(self):
        ''' TODO comment.
        '''
        if self._df0 is not None: 
            log0.debug(f'\nself._df0\n\n {self._df0}')


    def read_db_test(self):
        ''' TODO comment.
        '''
        log0.debug(f'etl')
        self.sql_exec(sel0)
        self.print_df0()


    def run_db_test(self):
        ''' TODO comment.
        '''
        log0.debug(f'run')
        self.con_open()
        try: 
            self.read_db_test()
        except Exception as e1: 
            log0.error(f'exception e1 {e1}')
        self.con_close()


    def gen_ins_pay0(self,pay0,fil0):
        ''' TODO comment.
        '''
        ret_val = f'insert into stg.pay0 (payload,dw_file) values '

        for i0,a0 in enumerate(pay0):
            com0 = ',' if i0 > 0 else ''
            ret_val += f"{com0}('{a0}','{fil0}')"

        return ret_val 


    def gen_ins_pay10(self,pay10,fil0):
        ''' TODO comment.
        '''
        ret_val = f'insert into stg.pay10 (payload,dw_file) values '

        for i0,a0 in enumerate(pay10):
            com0 = ',' if i0 > 0 else ''
            ret_val += f"{com0}('{a0}','{fil0}')"

        return ret_val 


    def gen_ins_pay20(self,pay20,fil0):
        ''' TODO comment.
        '''
        ret_val = f'insert into stg.pay20 (payload,dw_file) values '

        for i0,a0 in enumerate(pay20):
            com0 = ',' if i0 > 0 else ''
            ret_val += f"{com0}('{a0}','{fil0}')"

        return ret_val 


    def etl_files_load(self):
        ''' TODO comment.
        '''
        log0.debug(f'etl_files_load()')

        self.fs_scan()
        log0.debug(f' self._files { self._files } ')
        log0.debug(f' self._pth0 { self._pth0 } ')
        for i0,a0 in enumerate(self._files):
            log0.debug(f'{i0} {a0}')
            #print(f'{i0} {a0}')

        #log0.debug(f'etl_files')
        #if True: return None
        for i0,a0 in enumerate(self._files):
            if a0.find('load_1_hr') < 0: continue
            #if i0 != 2: continue

            #if i0 == 2: continue
            #else: break
            log0.debug(f' <!!>{i0} {a0}')
            pth1  = self._pth0 + a0
            pay20 = []
            with open(pth1,'r') as f0:
                lin0 = f0.readlines()

            for i1,a1 in enumerate(lin0):
                a2 = a1.replace('\n','')
                #log0.debug(f' {i0} {i1} {a0} {a2}')
                pay20.append(a2)

            ins20 = self.gen_ins_pay20(pay20,a0)
            #log0.debug(f'ins0 {ins0}')
            try:
                self.sql_exec(del20)
                self.sql_exec(ins20)
                #self.sql_exec(sel1)
                self.sql_exec(ins21)
                #self.sql_exec(sel2)
                #self.sql_exec(sel9)
                #self.print_df0()
            except Exception as e4:
                log0.debug(f'con_close e4 {e4}')
            log0.debug(f'<*> {i0} {a0}')
            #if True: return
            






    def etl_files_fuel_mix(self):
        ''' TODO comment.
        '''
        log0.debug(f'etl_files_fuel_mix()')

        self.fs_scan()
        log0.debug(f' self._files { self._files } ')
        log0.debug(f' self._pth0 { self._pth0 } ')
        for i0,a0 in enumerate(self._files):
            log0.debug(f'{i0} {a0}')
            #print(f'{i0} {a0}')

        #log0.debug(f'etl_files')
        #if True: return None
        for i0,a0 in enumerate(self._files):
            if a0.find('fuel_mix_1_hr') < 0: continue
            #if i0 != 2: continue

            #if i0 == 2: continue
            #else: break
            log0.debug(f' <!!>{i0} {a0}')
            pth1  = self._pth0 + a0
            pay10 = []
            with open(pth1,'r') as f0:
                lin0 = f0.readlines()

            for i1,a1 in enumerate(lin0):
                a2 = a1.replace('\n','')
                #log0.debug(f' {i0} {i1} {a0} {a2}')
                pay10.append(a2)

            ins10 = self.gen_ins_pay10(pay10,a0)
            #log0.debug(f'ins0 {ins0}')
            try:
                self.sql_exec(del10)
                self.sql_exec(ins10)
                #self.sql_exec(sel1)
                self.sql_exec(ins11)
                #self.sql_exec(sel2)
                #self.sql_exec(sel9)
                #self.print_df0()
            except Exception as e4:
                log0.debug(f'con_close e4 {e4}')
            log0.debug(f'<*> {i0} {a0}')
            #if True: return
            




    def etl_files_ems(self):
        ''' TODO comment.
        '''
        log0.debug(f'etl_files_ems()')
        self.fs_scan()
        log0.debug(f' self._files { self._files } ')
        log0.debug(f' self._pth0 { self._pth0 } ')
        for i0,a0 in enumerate(self._files):
            log0.debug(f'{i0} {a0}')
            #print(f'{i0} {a0}')

        #log0.debug(f'etl_files')
        #if True: return None
        for i0,a0 in enumerate(self._files):
            if a0.find('ems') < 0: continue
            #if i0 != 2: continue

            #if i0 == 2: continue
            #else: break
            log0.debug(f' <!!>{i0} {a0}')
            pth1  = self._pth0 + a0
            pay0 = []
            with open(pth1,'r') as f0:
                lin0 = f0.readlines()

            for i1,a1 in enumerate(lin0):
                a2 = a1.replace('\n','')
                #log0.debug(f' {i0} {i1} {a0} {a2}')
                pay0.append(a2)

            ins0 = self.gen_ins_pay0(pay0,a0)
            #log0.debug(f'ins0 {ins0}')
            try:
                self.sql_exec(del0)
                self.sql_exec(ins0)
                self.sql_exec(sel1)
                self.sql_exec(ins1)
                self.sql_exec(sel2)
                self.sql_exec(sel9)
                #self.print_df0()
            except Exception as e4:
                log0.debug(f'con_close e4 {e4}')
            log0.debug(f'<*> {i0} {a0}')
            #if True: return
            

    def run(self):
        ''' TODO comment.
        '''
        log0.debug(f'run')
        self.con_open()
        try: 
            self.etl_files_ems()
            self.etl_files_fuel_mix()
            self.etl_files_load()
        except Exception as e1: 
            log0.error(f'exception e1 {e1}')
        self.con_close()



def main():#
    log0.debug(f'main()')
    try: 
        #etl0 = ETL_HF2()
        etl0 = ETLA()
        #etl0.run_db_test()
        etl0.run()
        sys.exit(0)
    except Exception as e0: 
        log0.error(f'exception e0 {e0}')
        sys.exit(1)
    

if __name__ == '__main__':
    main()

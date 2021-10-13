import pandas as pd 

## Load stata data files as data frames
may_rand = pd.read_stata("voterfile/vr_2021_may_sb_bday_vf.dta")
june_rand = pd.read_stata("voterfile/vr_2021_jun_sb_bday_vf.dta")
july_rand = pd.read_stata("voterfile/vr_2021_jul_sb_bday_vf.dta")

may_exp = pd.read_stata("export/vr_2021_may_sb_bday_export.dta")
june_exp = pd.read_stata("export/vr_2021_jun_sb_bday_export.dta")
july_exp = pd.read_stata("export/vr_2021_jul_sb_export.dta")

## remove excluded from july
set(july_exp['program'])
july_exp = july_exp[july_exp.program != 'Unknown']
july_exp['program'].value_counts()
july_exp['program'].isna().sum()
july_exp.shape

## Subset each file to only the columns they have in common

acols = june_exp.columns.intersection(may_exp.columns)
bcols = june_exp.columns.intersection(july_exp.columns)
innercols = acols.intersection(bcols)
len(innercols)

e_june_clean = june_exp[innercols]
e_july_clean = july_exp[innercols]
e_may_clean = may_exp[innercols]

## check column match
sum((e_june_clean.dtypes == e_may_clean.dtypes))
sum((e_july_clean.dtypes == e_may_clean.dtypes))

sameclass1 = e_june_clean.dtypes == e_may_clean.dtypes
sameclass1.value_counts()

sameclass2 = e_july_clean.dtypes == e_may_clean.dtypes
sameclass2.value_counts()

## identify variables of different classes 
notsame = sameclass2[~sameclass2].index.tolist()

## change type of mismatching columns
convert_dict = {'voterbase_id' : object, 'ck_dgt'  : float, 'hs_grad_yr' : float,
'age_asl' : float, 'electiondayage_asl' : float, 'census_county_code_asl' : float,
'fnl_zip' : float, 'fnl_zip4' : float, 'x1_temp_hh_id' : float, 'x1_household_id' : float}

e_july_cols = e_july_clean[notsame].astype(convert_dict)
e_june_cols = e_june_clean[notsame].astype(convert_dict)
e_may_cols = e_may_clean[notsame].astype(convert_dict)

e_july_cleana = e_july_clean.drop(notsame, axis=1)
e_june_cleana = e_june_clean.drop(notsame, axis=1)
e_may_cleana = e_may_clean.drop(notsame, axis=1)

e_july_clean1 = pd.concat([e_july_cleana, e_july_cols], axis=1)
e_june_clean1 = pd.concat([e_june_cleana, e_june_cols], axis=1)
e_may_clean1 = pd.concat([e_may_cleana, e_may_cols], axis=1)

set(e_july_clean1.columns) == set(e_june_clean1.columns) == set(e_may_clean1.columns) 
set(e_july_clean1.dtypes) == set(e_june_clean1.dtypes) == set(e_may_clean1.dtypes) 

## process voterfile data for each month 
may_rand.columns
june_rand.columns
july_rand.columns

r_may_clean = may_rand[['x1_id','_merge_wave','experiment', 'condition', 'treat', 'x1_temp_hh_id','c4_in_hh', 'bc_race_black', 'bc_race_hispanic', 'bc_race_white', 'bc_female', 'x1_household_id', 'household_member', 'exp_chase_grp','sms_condition', 'sms_treat', 'bc_sms_treat', 'sms_ctrl', 'exp_bday_grp', 'ctrl']]

r_june_clean = june_rand[['x1_id','_merge_wave','experiment', 'condition', 'treat','x1_temp_hh_id', 'c4_in_hh', 'bc_race_black', 'bc_race_hispanic', 'bc_race_white', 'bc_female', 'x1_household_id', 'household_member', 'exp_chase_grp', 'sms_condition', 'sms_treat', 'bc_sms_treat', 'sms_ctrl', 'exp_bday_grp', 'ctrl']]

r_july_clean = july_rand[['x1_id','experiment', 'condition', 'treat', 'x1_temp_hh_id', 'c4_in_hh', 'bc_race_black', 'bc_race_hispanic', 'bc_race_white', 'bc_female', 'x1_household_id', 'household_member', 'exp_chase_grp', 'sms_condition', 'sms_treat', 'bc_sms_treat', 'sms_ctrl', 'exp_bday_grp', 'ctrl']]

unique_may_cols = set(r_may_clean.columns)
unique_june_cols = set(r_june_clean.columns)
unique_july_cols = set(r_july_clean.columns)

unique_may_cols - unique_june_cols
unique_june_cols - unique_may_cols

unique_may_cols - unique_july_cols
unique_july_cols - unique_may_cols

r_july_clean = r_july_clean.reindex(columns = r_july_clean.columns.tolist() + ['_merge_wave'])
r_july_clean = r_july_clean[r_may_clean.columns]
r_july_clean['_merge_wave'] = r_july_clean['_merge_wave'].astype('category')


set(r_may_clean.columns) == set(r_june_clean.columns) == set(r_july_clean.columns)
set(r_may_clean.dtypes) == set(r_june_clean.dtypes) == set(r_july_clean.dtypes)


## merge together export and voterfile data for each month
mayids1 = set(e_may_clean1['x1_id'])
mayids2 = set(r_may_clean['x1_id'])
mayidscommon = mayids1.intersection(mayids2)
len(mayidscommon)

juneids1 = set(e_june_clean1['x1_id'])
juneids2 = set(r_june_clean['x1_id'])
juneidscommon = juneids1.intersection(juneids2)
len(juneidscommon)

julyids1 = set(e_july_clean1['x1_id'])
julyids2 = set(r_july_clean['x1_id'])
julyidscommon = julyids1.intersection(julyids2)
len(julyidscommon)

len(julyidscommon) + len(juneidscommon) + len(mayidscommon)

may_clean = pd.merge(e_may_clean1, r_may_clean, how='outer', on='x1_id', suffixes = ('_mexp', '_rand'))
june_clean = pd.merge(e_june_clean1, r_june_clean, how='outer', on='x1_id', suffixes = ('_mexp', '_rand'))
july_clean = pd.merge(e_july_clean1, r_july_clean, how='outer', on='x1_id', suffixes = ('_mexp', '_rand'))

may_clean['month'] = "may"
june_clean['month'] = "june"
july_clean['month'] = "july"

may_clean.shape
june_clean.shape
july_clean.shape

set(may_clean.columns) == set(june_clean.columns) == set(july_clean.columns)
set(may_clean.dtypes) == set(june_clean.dtypes) == set(july_clean.dtypes)

## concatenate months data 

y2021_may_jul_bday = pd.concat([may_clean, june_clean, july_clean], axis=0, ignore_index=True)
y2021_may_jul_bday.shape 
y2021_may_jul_bday.dtypes
len(set(y2021_may_jul_bday['x1_id']))

y2021_may_jul_bday.info(verbose=True)

y2021_may_jul_bday['rand_condition_month_21'] = y2021_may_jul_bday['condition_rand'] + ' ' + y2021_may_jul_bday['month'].astype(str)

y2021_may_jul_bday['mexp_condition_month_21'] = y2021_may_jul_bday['condition_mexp'] + ' ' + y2021_may_jul_bday['month'].astype(str)

## export

y2021_may_jul_bday.to_csv('vr_sb_bday_mailed_thru_jul_2021.csv', index=False, sep='\t')



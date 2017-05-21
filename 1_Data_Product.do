*This Code is producing the Data Product 1 for the professional CEO Project
/*[Data Product 1: A longitudinal list of CEO spells for all Kft, Bt, Ert. 
If the firm has, at any point of time, more than one CEO, drop the entire firm.
 A firm is a Frame id. Rows are spells. Columns are frame_id, person_id, start_date, end_date.]
*/

clear all
set more off

cap log using ${LOG}1_Data_Product, replace

cd ${DATA}
insheet using managerdb_with_dates.csv
*use ${DATA}managerdb_with_dates, clear
cap drop tag
egen tag = tag(frame_id)

*Number of frames:
count if tag == 1

replace end_date ="" if end_date == "9999-12-31"

foreach x of varlist start_date end_date {
	g  `x'_date = date(`x', "YMD")
	format `x'_date %td
	drop `x'	
}
*

tab pos5, m

** Dropping those who are not in CEO position (including missings!)
drop if pos5 != 1
sort frame_id start_date
drop tag
egen tag = tag(frame_id)

count if mi(start_date_date )
count if mi(end_date_date )

*There is no missing start/end dates, so if there is a  next start date < current end date
*there is an overlap 

g next_start_date = start_date[_n+1] if frame_id == frame_id[_n+1]
g byte overlap = next_start_date < end_date_date 
egen frame_w_overlap = max(overlap ), by(frame_id )

** Keeping only firms with 1 CEO at any point in time

keep if frame_w_overlap == 0

*Number of frames:
count if tag == 1

*662377 unique frame_id

duplicates r frame_id person_id start_date 

keep frame_id person_id start_date_date end_date_date 

rename start_date_date start_date 
rename end_date_date end_date

save ${OUTPUT}Data_Product_1.dta,replace

log close

cd ${OUTPUT}


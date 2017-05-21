/* [Data Product 2: A list of founders for each Frame id.
 Given the birth date of the firm (let's use the frame.csv for the birth date), select person_ids that have at least 30 days of overlap
with the first year of the firm from the following categories: rovat_13, rovat_15, rovat_109, rovat_106, rovat_206, rovat_110.
     Output can be JSON: with frame_ids as dictionary keys, list all the person_ids of founders.
 Alternatively, a csv of founder-firm matches.]
*/

clear all
set more off

cap log using ${LOG}2_Data_Product, replace

cd ${TAX_FRAME}

insheet using frame.csv
*use frame, clear
cap drop notnumeric 

tostring(ceg_id), replace
gen byte notnumeric = real(ceg_id )==.
tab notnumeric

drop if notnumeric == 1
destring ceg_id, replace

sort ceg_id

save ${OUTPUT}frame.dta, replace

insheet using ${OWNER}ownership_mixed_directed_w_mock_ids_w_sources.csv, clear
*use ${DATA}ownership_mixed_directed_w_mock_ids_w_sources, clear

drop if person_id_owner == ""
keep if rovat == 106 | rovat == 109 | rovat == 206 | rovat == 110

merge m:1 tax_id ceg_id using ${OUTPUT}frame, gen(frame_merge)
drop if frame_merge == 2
drop if frame_merge == 1

ren person_id_owner person_id
cap drop is_* notnumeric 
cap drop _merge
cap drop mock_id_owner tax_id_owner alrovat_id

tempfile owners
save `owners'
save owners_temp, replace

*mandb2 containes rovat15!
cd ${MANDB2}
insheet using managerdb_with_dates.csv, clear
*use managerdb_with_dates, clear

drop frame_id alrovat_id group_prefix sex board pos5 

ren start_date start
ren end_date end

merge m:1 ceg_id tax_id using ${OUTPUT}frame, gen(m_f)

keep if m_f == 3
drop is_* notnumeric
cap drop _merge

replace rovat = substr(rovat, -2,.)
destring(rovat ), replace

append using `owners'

foreach x of varlist birth_date death_date   {
	gen str new   = string(`x', "%10.0g")
	gen  `x'_d = date(new, "YMD") 
	format `x'_d %td
	drop new `x'
}
*
foreach x of varlist start end { 
	g  `x'_date = date(`x', "YMD")
	format `x'_date %td
	drop `x'
} 
*
drop name 

save ${OUTPUT}Appended_mandb_owner.dta, replace

egen frame_id_start = min(birth_date_d), by(frame_id)
format frame_id_start %td

g anniversary = mdy(month(frame_id_start), day(frame_id_start), year(frame_id_start) + 1)
format anniversary %td


g distance_from_anniversary = anniversary - start_date 
g founder = 0 
replace founder = 1 if  distance_from_anniversary > 30 & distance_from_anniversary !=.
egen founder_frame = max(founder), by(frame_id)

cap drop tag
egen tag = tag(frame_id)

*The number of frames with and without founders
tab founder_frame if tag == 1

*Inconsistencies
count if start_date < frame_id_start & frame_id_start != .

keep if founder == 1
keep frame_id person_id 

duplicates drop

save ${OUTPUT}Data_product_2.dta, replace

log close










## master_data/master_column.py

master_column = [
    'Inspection Lot', 'Sample no', 'Ref. Sample No.', 'Old Code',
    'Material Code', 'Material Description', 'Truck no.', 'Pallet No.',
    'Batch No.', 'Formula', 'Date', 'EXCPTCP', 'MIN CP', 'DIFF CP',
    'DIFF MIN', 'MOIS', 'ASH', 'PROTEIN', 'FAT', 'FIBER', 'P', 'Ca',
    'INSOL', 'NaCl', 'Na', 'K', 'Fines', 'Durability', 'T_FAT',
    'Bulk density', 'Aw', 'Starch', '% cook', 'L*', 'a*', 'b*', 'Hardness',
    'ADF', 'ADL', 'NDF', 'ถัง', 'Load Time', 'Plant', 'Remark',
    'Validation Code', 'Valuation Date', 'CONCATENATE'
]

active_columns = [
    'Inspection Lot', 'Sample no', 'Old Code',
    'Material Code', 'Material Description', 'Truck no.', 'Pallet No.',
    'Batch No.', 'Formula', 'Date', 'EXCPTCP', 'MIN CP', 'DIFF CP',
    'DIFF MIN', 'MOIS', 'ASH', 'PROTEIN', 'FAT', 'FIBER', 'P', 'Ca',
    'INSOL', 'NaCl', 'Na', 'K', 'Fines', 'Durability', 'T_FAT',
    'Bulk density', 'Aw', 'Starch', '% cook', 'L*', 'a*', 'b*', 'Hardness',
    'ADF', 'ADL', 'NDF', 'ถัง', 'Load Time', 'Plant', 'Remark',
    'Validation Code', 'Valuation Date'
]

new_column = [
    'inspection_lot', 'sample_no', 'material_old_code',
    'material_code', 'material_description', 'truck_no', 'pallet_no',
    'batch_no', 'formula_name', 'manufacturing_date', 'excptcp', 'min_cp', 'diff_cp',
    'diff_min', 'moisture', 'ash', 'protein', 'fat', 'fiber', 'p', 'ca',
    'insoluble', 'nacl', 'na', 'k', 'fines', 'durability', 't_fat',
    'bulk_density', 'aw', 'starch', 'cook', 'l_star', 'a_star', 'b_star', 'hardness',
    'adf', 'adl', 'ndf', 'bin_no', 'load_time', 'plant', 'remark',
    'validation_code', 'validation_date'
]

numeric_cols = [
        'excptcp', 'min_cp', 'diff_cp', 'diff_min', 'moisture', 'ash', 'protein', 'fat',
        'fiber', 'p', 'ca', 'insoluble', 'nacl', 'na', 'k', 'fines', 'durability', 
        't_fat', 'bulk_density', 'aw', 'starch', 'cook', 'l_star', 'a_star', 'b_star', 'hardness', 
        'adf', 'adl', 'ndf'
    ]

string_cols = [
        'inspection_lot', 'sample_no', 'material_old_code', 'material_code', 'material_description',
        'truck_no', 'pallet_no', 'batch_no', 'formula_name', 'bin_no', 'plant', 'remark', 'validation_code'
    ]

date_cols = ['manufacturing_date', 'validation_date']

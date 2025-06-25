import os
import pandas as pd
import re
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl.styles.stylesheet')
from Awam import read_ne_report, find_df_diff, read_file, filter_df_ne
from report_summary import generate_final_report

def main():
    downloads_folder = os.path.expanduser("~/Downloads")
    old_report = os.path.join(downloads_folder, 'NE Report_2025-04-06_11-37-27_0.xlsx')
    dfold = read_ne_report(old_report)
    dfold_filtered = filter_df_ne(dfold)
    ########################
    current_report = os.path.join(downloads_folder, 'NE Report_2025-06-24_09-55-13_0.xlsx')
    dfnew = read_ne_report(current_report)
    dfnew_filtered = filter_df_ne(dfnew)
    ##############################################################################
    init_folder = 'Reports-2025-04-06_11-37'
    init_l3dcn_df = pd.read_excel(os.path.join(init_folder, 'L3DCN-2025-04-06_11-37.xlsx'))
    init_migrated_l3dcn_ncode = init_l3dcn_df[init_l3dcn_df['Migration Status'] == 'Migrated']['NCode'].tolist()
    ##############################################################################
    old_date_match = re.search(r'(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2})', old_report)
    date_part = old_date_match.group(1)
    time_part = f"{old_date_match.group(2)}:{old_date_match.group(3)}:{old_date_match.group(4)}"
    full_report_date = pd.to_datetime(f"{date_part} {time_part}", errors='coerce')
    #######################
    date_match = re.search(r'((\d{4}-\d{2}-\d{2})_\d{2}-\d{2}-\d{2}_\d+)', current_report)
    if date_match:
        report_date = date_match.group(2)
        report_folder = os.path.join(f'Reports-{report_date}')
        os.makedirs(report_folder, exist_ok=True)
        print(f'Created {report_folder}')
    else:
        raise ValueError("Date not found in current_report path")
    ##############################################################################
    dfnew_unique = find_df_diff(dfnew, dfold)
    filtered_dfnew_unique = filter_df_ne(dfnew_unique)
    dfold_unique = find_df_diff(dfold, dfnew)
    ##############
    dfnew_poc1 = dfnew_filtered['POC1']
    dfnew_poc13 = dfnew_filtered['POC13']
    dfnew_rr = dfnew_filtered['RR']
    dfnew_poc2 = dfnew_filtered['POC2']
    dfnew_ptn_poc2 = dfnew_filtered['PTN_POC2']
    dfnew_m8c = dfnew_filtered['M8C']
    dfnew_m14 = dfnew_filtered['M14']
    dfnew_m8c_m14 = dfnew_filtered['M8C_M14']
    dfnew_x2 = dfnew_filtered['X2']
    dfnew_x3 = dfnew_filtered['X3']
    dfnew_x2_x3 = dfnew_filtered['X2_X3']
    dfnew_atn_poc2 = dfnew_filtered['ATN_POC2']
    ########################################################################
    ########################################################################
    dfnew_poc1.to_excel(os.path.join(report_folder, f'Current_POC1-{report_date}.xlsx'), index=False)
    print('Current_POC1: ✅')
    ########################
    dfnew_rr.to_excel(os.path.join(report_folder, f'Current_RR-{report_date}.xlsx'), index=False)
    print('Current_RR: ✅')
    ########################
    dfnew_poc13.to_excel(os.path.join(report_folder, f'Current_POC13-{report_date}.xlsx'), index=False)
    print('Current_POC13: ✅')
    ########################
    dfnew_x2.to_excel(os.path.join(report_folder, f'Current_X2-{report_date}.xlsx'), index=False)
    print('Current_X2: ✅')
    ########################
    dfnew_x3.to_excel(os.path.join(report_folder, f'Current_X3-{report_date}.xlsx'), index=False)
    print('Current_X3: ✅')
    ########################
    dfnew_m8c.to_excel(os.path.join(report_folder, f'Current_M8C-{report_date}.xlsx'), index=False)
    print('Current_M8C: ✅')
    ########################
    dfnew_m14.to_excel(os.path.join(report_folder, f'Current_M14-{report_date}.xlsx'), index=False)
    print('Current_M14: ✅')
    ########################
    dfnew_atn_poc2.to_excel(os.path.join(report_folder, f'Current_ATN_POC2-{report_date}.xlsx'), index=False)
    print('Current_ATN_POC2: ✅')
##############################################################################
##############################################################################
    dfnew_atn_poc2.to_excel(os.path.join(report_folder, f'Current_ATN_POC2-{report_date}.xlsx'), index=False)
    print('Current_ATN_POC2: ✅')
    dfnew_poc3 = dfnew_filtered['POC3']
    dfnew_dcn = dfnew_filtered['DCN']
    dfnew_dcn_mtx = dfnew_filtered['DCN_MTX']
    dfnew_dcn_hubs = dfnew_filtered['DCN_HUBS']
    dfnew_gps = dfnew_filtered['GPS']
    dfnew_910cg = dfnew_filtered['ATN910CG']
    ##############################################################################
    dfnew_dismantled_dcn = dfold_unique[dfold_unique['NE Name'].str.contains('ATN_DCN', case=False)]
    dcn_migration_status = []
    dcn_dismantle_status = []
    dcn_vpn_insts = []
    mtx_names = ['HQ', 'MOK', 'MKT', 'RMD', 'ALX', 'ZHR', 'CA4', 'MNS', 'BS', 'CAI5', 'CA5', 'TNT']
    for ncode in dfnew_dcn_hubs['NCode'].tolist():
        dcn_vpn_inst = None
        poc2_names = dfnew_poc2[dfnew_poc2['NE Name'].str.contains(rf'^{ncode}', case=False, na=False)][
            'NE Name'].tolist()
        print(poc2_names)
        if len(poc2_names) > 1:
            poc2_names = [x for x in poc2_names if x in dfnew_poc2['NE Name'].tolist()]
            print(poc2_names)
        if poc2_names:
            poc2_name = poc2_names[0]
            content, chunks = read_file(f'{poc2_name}.txt')
            for chunk in chunks:
                match = re.search(r'^ip vpn-instance (.*?)_DCN_O&M', chunk)
                if match and re.search(r'route-distinguisher 350:\d\n', chunk) \
                and not re.search(r'^ip vpn-instance (HQ|MOK|MKT|RMD|ALX|ZHR|CA4|MNS|BS|CAI5|CA5)_DCN_O&M', chunk, flags=re.IGNORECASE):
                    dcn_vpn_inst = match.group()
                    dcn_vpn_inst = [dcn_vpn_inst]
                    print(chunk)
                    break
            dcn_vpn_insts.append(dcn_vpn_inst)
            print(dcn_vpn_inst)
            dcn_migration_status.append('Migrated' if isinstance(dcn_vpn_inst, list) else 'Not Migrated')
            dcn_dismantle_status.append(
                'Dismantled' if poc2_name in dfnew_dismantled_dcn['NE Name'].tolist() else "Not Dismantled")
        else:
            dcn_vpn_insts.append(None)
            dcn_migration_status.append("Missing POC2")
            dcn_dismantle_status.append("Not Dismantled")
        print('=====================')
    dfnew_dcn_hubs.insert(2, 'Migration Status', dcn_migration_status)
    dfnew_dcn_hubs.insert(1, 'VPN Instance', dcn_vpn_insts)
    dfnew_dcn_hubs.insert(3, 'Dismantle Status', dcn_dismantle_status)
    ##############################################################################
    dfnew_dismantled_dcn.insert(2, 'Migration Status', 'Migrated')
    dfnew_dismantled_dcn.insert(1, 'VPN Instance', None)
    dfnew_dismantled_dcn.insert(3, 'Dismantle Status', 'Dismantled')
    dfnew_dcn_hubs = pd.concat([dfnew_dismantled_dcn, dfnew_dcn_hubs], ignore_index=True)
    dfnew_dcn_hubs.loc[
        dfnew_dcn_hubs['NCode'].isin(init_migrated_l3dcn_ncode),
        'Migration Status'
    ] = 'Migrated Before'
    dfnew_dcn_hubs.to_excel(os.path.join(report_folder, f'L3DCN-{report_date}.xlsx'), index=False)
    print('L3DCN: ✅')
##############################################################################
##############################################################################
    df_dismantled_nodes = find_df_diff(dfold, dfnew)
    dict_dismantled_nodes = filter_df_ne(df_dismantled_nodes)
    df_dismantled_poc2 = dict_dismantled_nodes['PTN_POC2']
    df_dismantled_poc2 = find_df_diff(df_dismantled_poc2, dfnew, 'NCode')
    df_dismantled_poc2.to_excel(os.path.join(report_folder, f'Dismantled_POC2s-{report_date}.xlsx'), index=False)
    print('Dismantled_POC2s: ✅')
    ##########################
    df_dismantled_poc3 = dict_dismantled_nodes['POC3']
    df_dismantled_poc3 = find_df_diff(df_dismantled_poc3, dfnew, 'NCode')
    df_dismantled_poc3.to_excel(os.path.join(report_folder, f'Dismantled_POC3s-{report_date}.xlsx'), index=False)
    print('Dismantled_POC3s: ✅')
##############################################################################
##############################################################################
    df_poc2 = filter_df_ne(dfnew)['POC2']
    df_poc2_this_year = df_poc2[df_poc2['Created On'] > full_report_date].copy()
    dfold_ptn_poc2 = filter_df_ne(dfold)['POC2']
    modernization_data = []
    reallocation_data = []
    newhub_data = []
    namechange_data = []
    for index, row in df_poc2_this_year.iterrows():
        ne_name = row['NE Name']
        esn = row['ESN']
        ncode = row['NCode']
        ne_type = row['NE Type (MPU Type)']
        ne_type = re.sub(r'(-IOT|\([a-zA-Z0-9]*\))', '', ne_type)
        if ncode in dfold_ptn_poc2['NCode'].values:
            found_entry_row = dfold_ptn_poc2[dfold_ptn_poc2['NE Name'].str.contains(rf'^{ncode}')]
            old_type = found_entry_row['NE Type (MPU Type)'].iloc[0]
            old_type_clean = re.sub(r'(-IOT|\([a-zA-Z0-9]*\))', '', old_type)
            old_esn = found_entry_row['ESN'].iloc[0]
            old_name = found_entry_row['NE Name'].iloc[0]
            if esn != old_esn and ne_type != old_type_clean:
                modernization_data.append({
                    'NE Name': ne_name,
                    'NE Type (MPU Type)': ne_type,
                    'Software Version': row['Software Version'],
                    'Patch Version List': row['Patch Version List'],
                    'Created On': row['Created On'],
                    'ESN': esn,
                    'Old NE Name': old_name,
                    'Old NE Type': old_type,
                    'Old ESN': old_esn,
                })
            elif esn != old_esn and ne_type == old_type_clean:
                reallocation_data.append({
                    'NE Name': ne_name,
                    'NE Type (MPU Type)': ne_type,
                    'Software Version': row['Software Version'],
                    'Patch Version List': row['Patch Version List'],
                    'Created On': row['Created On'],
                    'ESN': esn,
                    'Old NE Name': old_name,
                    'Old NE Type': old_type,
                    'Old ESN': old_esn,
                })
        else:
            if esn in dfold_ptn_poc2['ESN'].values:
                old_name = dfold_ptn_poc2[dfold_ptn_poc2['ESN'] == esn]['NE Name'].iloc[0]
                namechange_data.append({
                    'NE Name': ne_name,
                    'NE Type (MPU Type)': ne_type,
                    'Software Version': row['Software Version'],
                    'Patch Version List': row['Patch Version List'],
                    'Created On': row['Created On'],
                    'ESN': esn,
                    'Old NE Name': old_name
                })
            else:
                newhub_data.append({
                    'NE Name': ne_name,
                    'NE Type (MPU Type)': ne_type,
                    'Software Version': row['Software Version'],
                    'Patch Version List': row['Patch Version List'],
                    'Created On': row['Created On'],
                    'ESN': esn,
                })
    ##########################
    df_modernization = pd.DataFrame(modernization_data)
    df_reallocation = pd.DataFrame(reallocation_data)
    df_newhub = pd.DataFrame(newhub_data)
    df_namechange = pd.DataFrame(namechange_data)
    df_modernization.to_excel(os.path.join(report_folder, f'POC2_Modernization-{report_date}.xlsx'), index=False)
    df_newhub.to_excel(os.path.join(report_folder, f'New_Hub-{report_date}.xlsx'), index=False)
    if not df_reallocation.empty:
        df_reallocation.to_excel(os.path.join(report_folder, f'Reallocation-{report_date}.xlsx'), index=False)
    if not df_namechange.empty:
        df_namechange.to_excel(os.path.join(report_folder, f'Change_Name{report_date}.xlsx'), index=False)

    print('POC2_Modernization: ✅')
    print('New_Hub: ✅')
    #########################
    dfnew_allmtx = pd.concat([dfnew_filtered['POC1'], dfnew_filtered['POC13'], dfnew_filtered['RR']], ignore_index=True)
    dfold_allmtx = pd.concat([dfold_filtered['POC1'], dfold_filtered['POC13'], dfold_filtered['RR']], ignore_index=True)
    mtx_modernization_data = []
    for _, mtx_row in dfnew_allmtx.iterrows():
        mtx_code = mtx_row['NCode']
        mtx_esn = mtx_row['ESN']
        mtx_type = mtx_row['NE Type (MPU Type)']
        mtx_type = re.sub(r'(-IOT|\([a-zA-Z0-9]*\))', '', mtx_type)
        mtx_role = re.search(r'(POC1[123]|MEC1[12]|RR[12])', str(mtx_row['NE Name']), flags=re.IGNORECASE).group(1)
        dfnew_allmtx.at[_, 'NCode'] = f'{mtx_code}_{mtx_role}'
        print('@', _, f'{mtx_code}_{mtx_role}')
        found_mtx_entry_row = dfold_allmtx[
            dfold_allmtx['NE Name'].str.contains(rf'^{mtx_code}', case=False, na=False) &
            dfold_allmtx['NE Name'].str.contains(rf'{mtx_role}', case=False, na=False)]
        if not found_mtx_entry_row.empty:
            print(found_mtx_entry_row)
            old_mtx_type = found_mtx_entry_row['NE Type (MPU Type)'].iloc[0]
            old_mtx_type = re.sub(r'(-IOT|\([a-zA-Z0-9]*\))', '', old_mtx_type)
            old_mtx_esn = found_mtx_entry_row['ESN'].iloc[0]
            old_mtx_name = found_mtx_entry_row['NE Name'].iloc[0]
            old_mtx_role = found_mtx_entry_row['NE Type (MPU Type)'].iloc[0]
            print(mtx_esn, old_mtx_esn, mtx_type, old_mtx_type, '\n')
            if mtx_esn != old_mtx_esn and mtx_type != old_mtx_type and mtx_role == old_mtx_role:
                mtx_migrated = 'Yes' if old_mtx_name not in dfnew_allmtx['NE Name'].tolist() else 'In Progress'
                print('$', mtx_migrated)
                mtx_modernization_data.append({
                    'NE Name': mtx_row['NE Name'],
                    'MTX Role': mtx_role,
                    'Completed?': mtx_migrated,
                    'NE Type (MPU Type)': mtx_type,
                    'Software Version': mtx_row['Software Version'],
                    'Patch Version List': mtx_row['Patch Version List'],
                    'Created On': mtx_row['Created On'],
                    'ESN': mtx_esn,
                    'Old NE Name': old_mtx_name,
                    'Old NE Type': old_mtx_type,
                    'Old ESN': old_mtx_esn,
                })
    df_mtx_modernization = pd.DataFrame(mtx_modernization_data)
    df_mtx_modernization.to_excel(os.path.join(report_folder, f'MTX_Modernization-{report_date}.xlsx'), index=False)
##############################################################################
##############################################################################
    dfnew_poc3 = filter_df_ne(dfnew)['POC3']
    dfold_poc3 = filter_df_ne(dfold)['POC3']
    dfnew_all_poc3s = dfnew_poc3.copy()
    data_poc3_mod = []
    check_basic = []
    check_cutover = []
    all_conns = []
    conn_types = ['SIAE', 'ML', 'TN', 'RTN', 'Eband', 'E-band', 'FTTS']
    for index, row in dfnew_poc3.iterrows():
        ne_name = row['NE Name']
        esn = row['ESN']
        ncode = row['NCode']
        current_mpu = row['NE Type (MPU Type)']
        current_mpu = re.sub(r'(-IOT|\([a-zA-Z0-9]*\))', '', current_mpu)
        if ncode in dfold_poc3['NCode'].values:
            found_entry_row = dfold_poc3[dfold_poc3['NE Name'].str.contains(rf'^{ncode}')]
            old_mpu = found_entry_row['NE Type (MPU Type)'].iloc[0]
            old_mpu = re.sub(r'(-IOT|\([a-zA-Z0-9]*\))', '', old_mpu)
            old_esn = found_entry_row['ESN'].iloc[0]
            old_name = found_entry_row['NE Name'].iloc[0]
            if esn != old_esn and current_mpu != old_mpu:
                data_poc3_mod.append({
                    'NE Name': ne_name,
                    'NE Type (MPU Type)': current_mpu,
                    'Software Version': row['Software Version'],
                    'Patch Version List': row['Patch Version List'],
                    'Created On': row['Created On'],
                    'ESN': esn,
                    'Old NE Name': old_name,
                    'Old NE Type': old_mpu,
                    'Old ESN': old_esn,
                })
        try:
            connections = []
            content, chunks = read_file(f"{ne_name}.txt")
            eth_numbers = [match.group(1) for chunk in chunks
                           if (match := re.search(r'interface Eth-Trunk(\d+)\..*?isis enable 11', chunk, re.DOTALL))]
            for eth_number in eth_numbers:
                desc_match = [match.group(1) for chunk in chunks
                              if (match := re.search(
                        fr'^interface GigabitEthernet\d+/\d+/\d+.*?description(.*?)\n.*?eth-trunk {eth_number}', chunk,
                        re.DOTALL))]
                if desc_match:
                    for conn in conn_types:
                        if re.search(rf'(^|[^a-zA-Z]){re.escape(conn)}([^a-zA-Z]|$)', desc_match[0],
                                     flags=re.IGNORECASE):
                            if conn.lower() == 'e-band':
                                conn = 'Eband'
                            connections.append(conn)
                            break
            connections = list(dict.fromkeys(connections))
            all_conns.append(connections)
            found_basic = re.search('bgp 65000', content, flags=re.IGNORECASE)
            check_basic.append('Yes' if found_basic else 'No')
            found_cutover = [
                chunk for chunk in chunks
                if re.search(r'^interface (GigabitEthernet\d+/\d+/\d+|Eth-Trunk\d+)\.', chunk) and
                   re.search(' ip binding vpn-instance ', chunk)
            ]
            check_cutover.append('Yes' if found_cutover else 'No')
        except:
            all_conns.append([])
            check_basic.append('Conf File is Missing')
            check_cutover.append('Conf File is Missing')
    dfnew_all_poc3s.insert(2, 'Basic Check', check_basic)
    dfnew_all_poc3s.insert(3, 'Cutover Check', check_cutover)
    dfnew_all_poc3s.insert(4, 'Connection Types', all_conns)
    dfnew_all_poc3s.to_excel(os.path.join(report_folder, f'POC3s-{report_date}.xlsx'), index=False)
    print('POC3s: ✅')
    df_poc3_mod = pd.DataFrame(data_poc3_mod)
    df_poc3_mod.to_excel(os.path.join(report_folder, f'POC3_Modernization-{report_date}.xlsx'), index=False)
    print('POC3_Modernization: ✅')
##############################################################################
##############################################################################
    found_time_synch = []
    for ne_name in dfnew['NE Name'].tolist():
        try:
            content, chunks = read_file(f"{ne_name}.txt")
            check_time_synch = [chunk for chunk in chunks if re.search('ptp profile g-8275-1 enable', chunk,
                                                                       flags=re.IGNORECASE)]
            found_time_synch.append('Yes' if check_time_synch else 'No')
        except:
            found_time_synch.append('Conf File is Missing')
    df_time_synch = dfnew.copy()
    df_time_synch.insert(2, 'Time Synch Check', found_time_synch)
    df_time_synch.to_excel(os.path.join(report_folder, f'Time_Synch-{report_date}.xlsx'), index=False)
    print('Time_Synch: ✅')
##############################################################################
##############################################################################
    check_green_power = []
    df_green_power = dfnew_m8c_m14.copy()
    for ne_name in df_green_power['NE Name'].tolist():
        try:
            content, chunks = read_file(f'{ne_name}.txt')
            found_green_power = re.search('set energy-saving mode deep warm-backup', content, flags=re.IGNORECASE)
            check_green_power.append('Yes' if found_green_power else 'No')
        except:
            check_green_power.append('Conf File is Missing')
    df_green_power.insert(2, 'Green_Power?', check_green_power)
    df_green_power.to_excel(os.path.join(report_folder, f'Green_Power-{report_date}.xlsx'), index=False)
    print('Green_Power: ✅')
#############################################################################
#############################################################################
    dfold_poc3 = filter_df_ne(dfold)['POC3']
    data_poc3_mod = []
    for ne_name in dfnew_poc3['NE Name'].tolist():
        ncode = re.split('[-_]', ne_name)[0]
        current_poc3_mpu = dfnew_poc3[dfnew_poc3['NE Name'].str.match(rf'^{ncode}', na=False)]['NE Type (MPU Type)'].iloc[0]
        current_poc3_mpu_cleaned = re.sub(r'\(.*?\)', '', current_poc3_mpu).strip()
        filtered_row = dfold_poc3[
            (dfold_poc3['NE Name'].str.match(rf'^{ncode}', na=False)) &
            (dfold_poc3['NE Type (MPU Type)'].str.contains('950B', na=False)) &
            (dfold_poc3['NE Type (MPU Type)'].apply(
                lambda x: re.sub(r'\(.*?\)', '', x).strip()) != current_poc3_mpu_cleaned)
            ]
        if not filtered_row.empty:
            found_in_dfold_poc3 = filtered_row['NE Name'].iloc[0]
            found_mputype = filtered_row['NE Type (MPU Type)'].iloc[0]
            data_poc3_mod.append({
                'NCode': ncode,
                'Old POC3 Name': found_in_dfold_poc3,
                'Old Node': found_mputype,
                'New POC3 Name': ne_name,
                'New Node': current_poc3_mpu,
            })
    df_poc3_mod = pd.DataFrame(data_poc3_mod)
    df_poc3_mod.to_excel(os.path.join(report_folder, f'POC3_Modernization-{report_date}.xlsx'), index=False)
    print('POC3_Modernization: ✅')
#############################################################################
#############################################################################
    final_df = generate_final_report(report_folder)
    output_file = os.path.join(report_folder, f"Final_Report_{report_date}.xlsx")
    final_df.to_excel(output_file, index=False)
    print(f"Final report generated: {output_file}")


if __name__ == "__main__":
    main()
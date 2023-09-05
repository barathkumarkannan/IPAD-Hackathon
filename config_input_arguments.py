import pandas as pd
import openpyxl
import json



def input_param_file_parser(base_dir, log):
    inb_pass_lst = []
    inb_fail_lst = []
    out_pass_lst = []
    out_fail_lst = []
    inputfile_df = pd.DataFrame(data=[], columns=[])
    inputfile_trans_df = pd.DataFrame(data=[], columns=[])
    inputfile_out_df = pd.DataFrame(data=[], columns=[])
    # Reading inputs from the input_parameters xlsx file
    try:
        ipfile_param_dfs = pd.read_excel(base_dir + 'input_parameters.xlsx', sheet_name='Parameters')
        if not ipfile_param_dfs.empty:
            ipfile_param_dfs = ipfile_param_dfs.dropna(how='all', axis=1)
            ipfile_param_dfs = ipfile_param_dfs.dropna(how='all', axis=0)
            ipfile_param_dfs.columns = ipfile_param_dfs.iloc[0]
            ipfile_param_dfs = ipfile_param_dfs.drop(ipfile_param_dfs.index[0])
            input_job_type = json.loads(ipfile_param_dfs.to_json(orient='records'))
            log.info("Driver workbook's  parameter sheet is read with below details")

            for i in range(len(input_job_type)):
                dict = input_job_type[i]
                if dict['Job Type'] == 'Inbound' and dict['Value'] == True:
                    log.info("User has given true flag for Inbound, hence framework is proceeding with respective data")
                    inputfile_df = pd.read_excel(base_dir + 'input_parameters.xlsx',sheet_name='Inbound')
                    jsonarr = json.loads(inputfile_df.to_json(orient='records'))
                    for index in range(len(jsonarr)):
                        tmp_dict = jsonarr[index]
                        if tmp_dict['Inbound'] == True and tmp_dict['Source Type'].lower() == 'file':
                            if tmp_dict['Source Directory'] is not None and tmp_dict['Source File Name'] is not None and tmp_dict['Source File Type'] is not None:
                                inb_pass_lst.append(tmp_dict['Source File Name'])
                            else:
                                inb_fail_lst.append(tmp_dict['Source File Name'])
                                inputfile_df = inputfile_df.drop(index)
                        elif tmp_dict['Inbound'] == True and tmp_dict['Source Type'].lower() == 'jdbc':
                            if tmp_dict['Source Database'] is not None and tmp_dict['Source Schema'] is not None and tmp_dict['Source Table'] and tmp_dict['Source URL'] is not None and tmp_dict['Source Hostname'] is not None and tmp_dict['Source Username'] is not None and tmp_dict['Source Password'] is not None:
                                inb_pass_lst.append(tmp_dict['Source Table'])
                            else:
                                inb_fail_lst.append(tmp_dict['Source Table'])
                                inputfile_df = inputfile_df.drop(index)
                        else:
                            inb_fail_lst.append(tmp_dict['Source File Name'])
                            inputfile_df = inputfile_df.drop(index)
                elif dict['Job Type'] == 'Transform' and dict['Value'] == True:
                    inputfile_trans_df = pd.read_excel(base_dir + 'input_parameters.xlsx', sheet_name='Transform')
                    trns_jsonarr = json.loads(inputfile_trans_df.to_json(orient='records'))
                    for tindex in range(len(trns_jsonarr)):
                        tmp_tr_dict = trns_jsonarr[tindex]
                        if tmp_tr_dict['Transform'] == True and tmp_tr_dict['File Path'] is not None:
                            log.info('Input in Transform sheet has no errors and is taken for execution')
                        else:
                            inputfile_trans_df.drop(tindex)
                elif dict['Job Type'] == 'Outbound' and dict['Value'] == True:
                    inputfile_out_df = pd.read_excel(base_dir + 'input_parameters.xlsx', sheet_name='Outbound')
                    out_jsonarr = json.loads(inputfile_out_df.to_json(orient='records'))
                    for index in range(len(out_jsonarr)):
                        tmp_out_dict = out_jsonarr[index]
                        if tmp_out_dict['Outbound'] == True and tmp_out_dict['Target Type'].lower() == 'file':
                            if tmp_out_dict['Target Directory'] is not None and tmp_out_dict['Target File Name'] is not None and tmp_out_dict['Stored As'] is not None:
                                out_pass_lst.append(tmp_out_dict['Target File Name'])
                            else:
                                out_fail_lst.append(tmp_out_dict['Target File Name'])
                                inputfile_out_df = inputfile_out_df.drop(index)
                        elif tmp_out_dict['Outbound'] == True and tmp_out_dict['Target Type'].lower() == 'jdbc':
                            if tmp_out_dict['Target Database'] is not None and tmp_out_dict['Target Table'] and tmp_out_dict['Target URL'] is not None and tmp_out_dict['Target Hostname'] is not None and tmp_out_dict['Target Username'] is not None and tmp_out_dict['Target Password'] is not None:
                                out_pass_lst.append(tmp_out_dict['Target Table'])
                            else:
                                out_fail_lst.append(tmp_out_dict['Target Table'])
                                inputfile_out_df = inputfile_out_df.drop(index)
                        elif tmp_out_dict['Outbound'] == True and tmp_out_dict['Target Type'].lower() == 'cloud':
                            if tmp_out_dict['Target Database'] is not None and tmp_out_dict['Target Table'] and tmp_out_dict['Target URL'] is not None and tmp_out_dict['Target Hostname'] is not None and tmp_out_dict['Target Username'] is not None and tmp_out_dict['Target Password'] is not None:
                                out_pass_lst.append(tmp_out_dict['Target Table'])
                            else:
                                out_fail_lst.append(tmp_out_dict['Target Table'])
                                inputfile_out_df = inputfile_out_df.drop(index)
                        else:
                            out_fail_lst.append(tmp_out_dict['Target File Name'])
                            inputfile_out_df = inputfile_out_df.drop(index)
            inb_pass_lst = [i for i in inb_pass_lst if i is not None]
            inb_fail_lst = [i for i in inb_fail_lst if i is not None]
            out_pass_lst = [i for i in out_pass_lst if i is not None]
            out_fail_lst = [i for i in out_fail_lst if i is not None]
            if len(inb_pass_lst) != 0:
                inb_pass_str = ", ".join(inb_pass_lst)
                log.info('Row(s) ' + inb_pass_str + ' of Inbound will be considered for execution')
            if len(inb_fail_lst) != 0:
                inb_fail_str = ", ".join(inb_fail_lst)
                log.info('Row(s) ' + inb_fail_str + ' of Inbound will not be considered, because mandatory inputs are missing')
            if len(out_pass_lst) != 0:
                out_pass_str = ", ".join(out_pass_lst)
                log.info('Row(s) ' + out_pass_str + ' of Outbound will be considered for execution')
            if len(out_fail_lst) != 0:
                out_fail_str = ", ".join(out_fail_lst)
                log.info('Row(s) ' + out_fail_str + ' of Outbound will not be considered, because mandatory inputs are missing')
        else:
            log.info("Parameters sheet has no value in it, hence aborting the execution of framework")
        log.info("Parameter workbook is validated successfully ")
    except Exception as e:
        log.error("Input file is not found under the directory or there is an error with file")
        log.error("An error occurred: {}".format(str(e)))
    return input_job_type, inputfile_df, inputfile_trans_df, inputfile_out_df






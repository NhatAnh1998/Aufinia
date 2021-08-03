import os, re
import glob
import datetime, dateparser
from datetime import *
import time
import csv
import xlrd
import psycopg2
from psycopg2.extensions import AsIs
from func_liblary import config, gdrive_files_download_condition, vn_imports_customs_raw_config, convert_date_string, send_email
from configparser import ConfigParser
import array as arr
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import statistics

# DATE_FORMAT = "%Y-%m-%d"
# DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
# TIME_FORMAT = "%H:%M"
# frw_type = 'Python: Import Research Data'
# file_name = os.path.basename(__file__)
start_time = time.time()

def save_data_raw(columns_list, file_name):
    conn = None
    params = config()
    conn = psycopg2.connect(**params)
    try:
        sql = "COPY nex.vn_imports_customs_raw (%s) FROM '%s' DELIMITER ',' CSV HEADER encoding 'UTF-8';" %(columns_list, file_name)
        print(sql)
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
              
        with conn.cursor() as cur:
            cur.execute("""DELETE FROM nex.vn_imports_customs_raw a USING nex.vn_imports_customs_raw b
                        WHERE a.id < b.id AND ((a.declaration_no = b.declaration_no and a.file_date <> b.file_date) 
                                    or (a.declaration_no = b.declaration_no and a.index_in_file = b.index_in_file));""")
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)#.format(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()
def load_DataFrames_keyword():
    try:
        engine = create_engine('postgresql://postgres:Dwh@2020@localhost:5432/Research') #keywords,result1,result2,result3,min_value,max_value
        dic_data = pd.read_sql_query("""SELECT * from nex.dictionary""",con=engine)
        # split HSCode DataFrame
        df_hscode = dic_data.loc[dic_data.dic_type == 'hscode', 'keywords':'result3']
        df_hscode.rename(columns={df_hscode.columns[0]:'hs_code',df_hscode.columns[1]:'GreenOrRoasted',
            df_hscode.columns[2]:'IsDecaf',df_hscode.columns[3]:'HsDetail'}, inplace=True)
        df_hscode['hs_code'] = df_hscode['hs_code'].astype(str) #Change column type Dataframe
        df_hscode = pd.DataFrame(df_hscode)
        # print(df_hscode)
        # print (df_hscode.dtypes)
        
        # split green_keywords DataFrame
        green_keywords = dic_data.loc[dic_data.dic_type == 'green_keywords', 'keywords']
        green_keywords = pd.DataFrame(green_keywords)
        
        # split roasted_keywords DataFrame
        roasted_keywords = dic_data.loc[dic_data.dic_type == 'roasted_keywords', 'keywords']
        roasted_keywords = pd.DataFrame(roasted_keywords)
        
        # split not_coffee_keywords DataFrame
        not_coffee_keywords = dic_data.loc[dic_data.dic_type == 'not_coffee_keywords', 'keywords']
        not_coffee_keywords = pd.DataFrame(not_coffee_keywords)
        
        # split robusta_keywords DataFrame
        robusta_keywords = dic_data.loc[dic_data.dic_type == 'robusta_keywords', 'keywords']
        robusta_keywords = pd.DataFrame(robusta_keywords)
        
        # split arabica_keywords DataFrame
        arabica_keywords = dic_data.loc[dic_data.dic_type == 'arabica_keywords', 'keywords']
        arabica_keywords = pd.DataFrame(arabica_keywords)
        
        # split excelsa_keywords DataFrame
        excelsa_keywords = dic_data.loc[dic_data.dic_type == 'excelsa_keywords', 'keywords']
        excelsa_keywords = pd.DataFrame(excelsa_keywords)

        # split packing_keywords DataFrame
        packing_keywords = dic_data.loc[dic_data.dic_type == 'packing_keywords', 'keywords':'result1']
        packing_keywords = pd.DataFrame(packing_keywords)

        # split UnitRef DataFrame
        UnitRef = dic_data.loc[dic_data.dic_type == 'UnitRef', 'keywords':'result1']
        UnitRef.rename(columns={UnitRef.columns[0]:'UnitCheck',UnitRef.columns[1]:'ConvertToMt'}, inplace=True)
        UnitRef = pd.DataFrame(UnitRef)

        # # split ref_price DataFrame
        # ref_price = dic_data.loc[dic_data.dic_type == 'ref_price', 'keywords':] #
        # ref_price = pd.DataFrame(ref_price)

        return (df_hscode, green_keywords, roasted_keywords, not_coffee_keywords, robusta_keywords, arabica_keywords, 
                excelsa_keywords, packing_keywords, UnitRef) #, ref_price
    except Exception as error:
        print(error)

if __name__ == '__main__':
    conn = None
    params = config()
    conn = psycopg2.connect(**params)
    print('Load dictionary DataFrame')
    df_hscode = load_DataFrames_keyword()[0]
    green_keywords = load_DataFrames_keyword()[1]
    roasted_keywords = load_DataFrames_keyword()[2]
    not_coffee_keywords = load_DataFrames_keyword()[3]
    robusta_keywords = load_DataFrames_keyword()[4]
    arabica_keywords = load_DataFrames_keyword()[5]
    excelsa_keywords = load_DataFrames_keyword()[6]
    packing_keywords = load_DataFrames_keyword()[7].reset_index(drop=True)
    UnitRef = pd.DataFrame(load_DataFrames_keyword()[8].reset_index(drop=True))
    # ref_price = load_DataFrames_keyword()[9]
    # ref_price['result2'] = ref_price['result2'].astype(str)
    # ref_price['min_value'] = ref_price['min_value'].astype(float)
    # ref_price['max_value'] = ref_price['max_value'].astype(float)
    # ref_price = ref_price.reset_index(drop=True)
    # ref_price = pd.DataFrame(ref_price)
    # print(packing_keywords)
    try:
        file_count = 0
        cur_path_file = path_save = ggd_folder_id = ''
        print('Load nearly Month from DB')
        with conn.cursor() as cur:
            cur.execute("""SELECT MAX(file_date) file_date FROM nex.vn_imports_customs_raw;""")
            db_date = cur.fetchone()[0]
            if db_date:
                db_date = datetime.strptime(str(db_date), '%Y%m')
            else:
                db_date = datetime.strptime('201001', '%Y%m') #now()
            # print(db_date)

        print('Load Google Drive path and store file destination path')
        for x in vn_imports_customs_raw_config():
            if x[0] == 'ggd_folder_id':
                ggd_folder_id = x[1]
            elif x[0] == 'save_path':
                path_save = os.path.expanduser('~') + '/research/global_sd/project_excutive/' + x[1]
        arr = os.listdir(path_save)
        if len(arr) > 0:
            for _file in filter(lambda x: '.' in x, arr):
                os.remove(path_save + _file)
        # Download files from google drive with date > db_date
        if len(gdrive_files_download_condition(ggd_folder_id, db_date, path_save, 'nk')) > 0:
            print('Load all columns structure of RAW table in DB')
            with conn.cursor() as cur:
                cur.execute("""SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = 'nex' 
                                AND TABLE_NAME = 'vn_imports_customs_raw';""")
                get_col_name = cur.fetchall()
                items_list = []
                for item in filter(lambda x: x[0] != 'id', get_col_name):
                    items_list.append(item[0])
                raw_cols_list = ", ".join(items_list)
                # print(raw_cols_list)
                # print(items_list)
            print('Load all columns structure of Cleaned table in DB')
            with conn.cursor() as cur:
                cur.execute("""SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = 'nex' 
                                AND TABLE_NAME = 'vn_imports_customs_cleaned';""")
                get_col_name = cur.fetchall()
                clean_items_list = []
                for item in filter(lambda x: x[0] != 'id', get_col_name):
                    clean_items_list.append(item[0])
                clean_cols_list = ", ".join(clean_items_list)

            print('Process data')
            arr = os.listdir(path_save)
            if len(arr) > 0:
                for file_str in filter(lambda r: "outlier" not in r.lower() and "nk" in r.lower() and (".xlsx" in r.lower() or ".xls" in r.lower()) and '.~lock' not in r, arr): #
                    try:
                        print(file_str)
                        read_file = pd.read_excel(path_save + file_str, sheet_name=0, header=0) #, delimiter=';'
                        if len(read_file.columns) != 62: send_email(os.path.basename(__file__), file_str, 'Please check columns number in VN Export Customes file')
                        read_file = read_file.reset_index()
                        # print(len(read_file.columns)) #Set DK kiem tra neu so Columns # 49 col thi gui mail
                        print('Rename all Columns of read_file same with DB columns name')
                        read_file.rename(columns={i:j for i,j in zip(read_file.columns,items_list[:len(items_list) - 1])}, inplace=True)
                        read_file['file_date'] = convert_date_string(file_str)[0]
                        print(read_file)
                        print('Saving Raw data file to DB')
                        read_file.to_csv(path_save + file_str.split('.')[0] + '.csv', index = None, header=True, encoding='UTF-8')
                        save_data_raw(raw_cols_list, path_save + file_str.split('.')[0] + '.csv')
                        read_file = read_file.drop('index_in_file', axis=1)

                        print('Starting processing data')
                        read_file['hs_code'] = read_file['hs_code'].astype(str)
                        # print(read_file)
                        df_merge_col = pd.merge(read_file, df_hscode, on='hs_code', how='left')
                        #Filter Green Keyword
                        cond_gr_keyword = df_merge_col.product_description.str.lower().str.contains(r'|'.join(green_keywords['keywords']))
                        #Filter Roasted Keyword
                        cond_rt_keyword = df_merge_col.product_description.str.lower().str.contains(r'|'.join(roasted_keywords['keywords']))
                        #Filter NotCoffee Keyword
                        cond_notcoffee_keyword = df_merge_col.product_description.str.lower().str.contains(r'|'.join(not_coffee_keywords['keywords']))

                        print(u'1. Lọc Green/Roasted/Not coffee và lọc Rô/A qua mô tả')
                        df_merge_col['Dc_GoR'] = np.where(cond_gr_keyword & cond_rt_keyword, 'Both',
                            (np.where(cond_gr_keyword | (cond_gr_keyword & cond_rt_keyword & (df_merge_col['GreenOrRoasted']=='Green')), 'Green',(np.where(cond_rt_keyword, 'Roasted','')))))
                        df_merge_col['NotCoffee'] = np.where(cond_notcoffee_keyword, 'Not Coffee', '')
                        df_merge_col['CategoryFinal'] = np.where(cond_notcoffee_keyword, 'Not Coffee',
                            (np.where(df_merge_col['Dc_GoR'] == '', df_merge_col['GreenOrRoasted'],
                            (np.where(df_merge_col['Dc_GoR'] == df_merge_col['GreenOrRoasted'], df_merge_col['GreenOrRoasted'],df_merge_col['Dc_GoR'])))))
                        
                        print(u'2. Tách thành 2 file Green/Roasted')
                        df_Roasted = df_merge_col[df_merge_col['CategoryFinal']!='Green']
                        df_Green = pd.DataFrame(df_merge_col[df_merge_col['CategoryFinal']=='Green'].reset_index(drop=True))
                        df_Roasted.to_csv(path_save + file_str.split('.')[0] + '_Roasted.csv', index = None, header=True, encoding='utf-8-sig') #windows-1251

                        #Filter Green Keyword
                        cond_ro_keywords = df_Green.product_description.str.lower().str.contains(r'|'.join(robusta_keywords['keywords']))
                        #Filter Roasted Keyword
                        cond_ar_keyword = df_Green.product_description.str.lower().str.contains(r'|'.join(arabica_keywords['keywords']))
                        #Filter NotCoffee Keyword
                        con_excelsa_keywords = df_Green.product_description.str.lower().str.contains(r'|'.join(excelsa_keywords['keywords']))
                        #Filter Packing Keyword
                        con_pk_keywords = df_Green.product_description.str.lower().str.contains(r'|'.join(packing_keywords['keywords']))

                        df_Green['Date'] = pd.to_datetime(df_Green['date_registrated'])
                        # print(df_Green)
                        # # print(df_Green[df_Green['date_registrated'].str.contains('/')])
                        # print(np.where(df_Green.date_registrated.str.match(r'/'), df_Green.date_registrated.str, 0))
                        # df_Green['Date'] = np.where(df_Green.date_registrated.str.match(r'/'), 
                        #     datetime.strptime(str(df_Green.date_registrated.str),'%d/%m/%Y').strftime('%Y-%m-%d'),
                        #     dateparser.parse(str(df_Green.date_registrated.str)).strftime('%Y-%m-%d'))
                        # df_Green['Date'] = dateparser.parse(df_Green['date_registrated'].astype(str).str).strftime('%Y-%m-%d')
                        print('2.1. Quy đổi date_registrated sang dạng Y-%m-%d và lưu tại Date field')
                        # df_Green['Date'] = df_Green['date_registrated'].apply(lambda x: datetime.strptime(str(x),'%d/%m/%Y').strftime('%Y-%m-%d') if not '00:00:00' in str(x) 
                        #     else dateparser.parse(str(x)).strftime('%Y-%m-%d'))
                        # print(df_Green.date_registrated.dtypes)
                        # print(df_Green['date_registrated'])
                        # df_Green['date_registrated'].apply(lambda x: datetime.strptime(str(x),'%d/%m/%Y').strftime('%Y-%m-%d') if not '00:00:00' in str(x) 
                        #     else dateparser.parse(str(x)).strftime('%Y-%m-%d'))
                        cols_str = df_Green.columns.difference(['product_unit','product_description','quantity','invoice_currency','invoice_amount','fx_rate'])
                        df_Green[cols_str] = df_Green[cols_str].astype(str)

                        print(u'3. Trong Green xác định TNE/MDW/MMC/TAM là các đơn vị tấn, Với các đơn vị KGM/KDW/KII là các đơn vị kg')
                        df_Green['product_unit'] = np.where((df_Green['product_unit'].str.lower().str.contains(r'|'.join(['mdw','mmc','tam']))),
                            'tne',np.where((df_Green['product_unit'].str.lower().str.contains(r'|'.join(['kdw','kii']))),'kgm',df_Green['product_unit']))
                        
                        print('3.1. Phân loại Robusta/Arabica dựa vào mô tả lần 1')
                        df_Green['VarietyFromDescription'] = np.where(cond_ro_keywords | (df_Green.product_description.str.lower() == u'cà phê#&'), 'Robusta',
                            (np.where(cond_ar_keyword, 'Arabica', (np.where(con_excelsa_keywords, 'Excelsa','')))))
                        
                        print('3.2. Tạo field Index để groupby trong bước sau')
                        df_Green = df_Green.reset_index()
                        print('3.3. Lấy keywords loại bao từ bảng packing_keywords nếu trong mô tả có chứa giá trị tương ứng')
                        #exctract values by packing_keywords['keywords'] match with product_description to new column
                        s = (df_Green.product_description.str.lower().str.extractall(f"({'|'.join(packing_keywords['keywords'])})")[0].rename('new').reset_index(level=1,drop=True))
                        df_Green = df_Green.join(s)
                        print('3.4. Map keywords loại bao field với packing_keywords để lấy đơn vị tính đã quy ước')
                        #repeat rows with duplicated match
                        df_Green['Unit_temp'] = df_Green['new'].map(packing_keywords.set_index('keywords')['result1'])
                        # #aggregate join
                        cols = [col for col in df_Green.columns if col not in ['new','Unit_temp']]
                        # print(cols)
                        df_temp = df_Green.drop('new', axis=1).fillna('Na').groupby(cols).first().reset_index().replace('Na', np.nan)
                        df_Green = df_temp.drop('index', axis=1)

                        print('3.5. Khởi tạo cột UnitCheck: nếu unit chuẩn kgm, tne, grm thì giữ nguyên, ngược lại lấy đơn vị quy ước')
                        df_Green['UnitCheck'] = np.where((df_Green.product_unit.str.lower().str.contains(r'|'.join(['kgm','tne','grm']))), 
                            df_Green['product_unit'].str.lower(),df_Green['Unit_temp']) #(np.where(con_pk_keywords, df_Green['Unit_temp'],'')))
                        # print(df_Green.loc[[29181]])
                        df_Green = pd.merge(df_Green, UnitRef, on='UnitCheck', how='left')#.drop(['result2'], axis=1)
                        
                        print('3.6. Quy đổi toàn bộ số lượng sang TNE nếu đã có Unit')
                        df_Green['MtAdjusted'] = df_Green['quantity'].astype(float) * df_Green['ConvertToMt'].astype(float)
                        # print('thay the gia tri NA thanh 0')
                        # df_Green['MtAdjusted'] = df_Green['MtAdjusted'].replace(np.nan, 0, regex=True)

                        # print(u'4.Đối với tờ khai có Unit chuẩn nhưng mô tả không rõ Ro/A thì tính giá USD/TNE và so sánh với phạm vi outliers để phân loại Ro/A')
                        print('3.7. Tính tỉ giá USD trung binh')
                        Avg_Fx_Rate = (df_Green[df_Green['invoice_currency']=='USD'])['fx_rate'].mean()
                        print('3.8.Tính giá USD/TNE')
                        df_Green['USD_per_TNE'] = np.where(df_Green['invoice_currency']=='USD', df_Green['invoice_amount']/df_Green['MtAdjusted'].astype(float),
                            (np.where(df_Green['invoice_currency']=='VND', (df_Green['invoice_amount']/Avg_Fx_Rate)/df_Green['MtAdjusted'].astype(float), 
                                ((df_Green['invoice_amount']*df_Green['fx_rate'])/Avg_Fx_Rate)/df_Green['MtAdjusted'].astype(float))))

                        print('3.9. Tính giá trị Median của Arabica/Robusta')
                        Median_Ara = statistics.median(df_Green.loc[(df_Green['VarietyFromDescription'] == 'Arabica'), 'USD_per_TNE'])
                        print('     Arabica Median : %s' %Median_Ara)
                        Median_Ro = statistics.median(df_Green.loc[(df_Green['VarietyFromDescription'] == 'Robusta'), 'USD_per_TNE'])
                        print('     Robusta Median : %s' %Median_Ro)

                        print('3.10. Xac dinh pham vi Outlier Arabica')
                        Q1_A = df_Green.loc[(df_Green['VarietyFromDescription'] == 'Arabica'), 'USD_per_TNE'].quantile(0.25)
                        Q3_A = df_Green.loc[(df_Green['VarietyFromDescription'] == 'Arabica'), 'USD_per_TNE'].quantile(0.75)
                        IQR_A = Q3_A - Q1_A
                        Q1_Ara = Q1_A - 1.5 * IQR_A
                        Q3_Ara = Q3_A + 1.5 * IQR_A
                        print('     Arabica range: IQR = %s, Q1 - 1.5 * IQR = %s, Q3 + 1.5 * IQR = %s' %(IQR_A, Q1_Ara, Q3_Ara))
                        print('3.11. Xac dinh pham vi Outlier Robusta')
                        Q1_R = df_Green.loc[(df_Green['VarietyFromDescription'] == 'Robusta'), 'USD_per_TNE'].quantile(0.25)
                        Q3_R = df_Green.loc[(df_Green['VarietyFromDescription'] == 'Robusta'), 'USD_per_TNE'].quantile(0.75)
                        IQR_R = Q3_R - Q1_R
                        Q1_Ro = Q1_R - 1.5 * IQR_R
                        Q3_Ro = Q3_R + 1.5 * IQR_R
                        print('     Robusta range: IQR = %s, Q1 - 1.5 * IQR = %s, Q3 + 1.5 * IQR = %s' %(IQR_R, Q1_Ro, Q3_Ro))
                        
                        print(u'4. Phân loại Arabica/Robusta dựa trên Non-Outlier đối với tờ khai mô tả không rõ R/A nhưng có Unit chuẩn.') #
                        df_Green['VarietyFinal'] = np.where(df_Green['VarietyFromDescription'] != '', df_Green['VarietyFromDescription'],# TH1: da phan loai A/R tai field VarietyFromDescription
                            (np.where((df_Green['USD_per_TNE'] > 0) & (df_Green['MtAdjusted'] > 0), #TH mô tả không rõ R/A nhưng có Unit chuẩn
                                (np.where((df_Green['USD_per_TNE'].astype(float) >= Q1_Ro) & 
                                    (df_Green['USD_per_TNE'].astype(float) <= Q3_Ro), 'Robusta',
                                (np.where((df_Green['USD_per_TNE'].astype(float) > Q3_Ro) & 
                                    (df_Green['USD_per_TNE'].astype(float) < Q3_Ara), 'Arabica','Outliers')))),'')))
                        
                        print(u'5. Phân loại Outlier có Unit chuẩn.') #
                        df_Green['CheckOutlier'] = np.where(df_Green['VarietyFinal'] == 'Outliers',df_Green['VarietyFinal'],
                            (np.where((df_Green['VarietyFinal']=='Robusta') & (df_Green['UnitCheck']!=''), #TH1: Xet Robusta va Units lay DonGia so voi Non-Outlier Ro de phan loai Outlier/A/R
                                (np.where((df_Green['USD_per_TNE'].astype(float) >= Q1_Ro) & 
                                    (df_Green['USD_per_TNE'].astype(float) <= Q3_Ro), 'Robusta',
                                (np.where((df_Green['USD_per_TNE'].astype(float) > Q3_Ro) & 
                                    (df_Green['USD_per_TNE'].astype(float) <= Q3_Ara), 'Arabica','Outliers')))),
                                (np.where((df_Green['VarietyFinal']=='Arabica') & (df_Green['UnitCheck']!=''), #TH2: Xet Arabica va Units lay DonGia so voi Non-Outlier A de phan loai Outlier/A/R
                                    (np.where((df_Green['USD_per_TNE'].astype(float) >= Q1_Ara) & 
                                        (df_Green['USD_per_TNE'].astype(float) <= Q3_Ara), 'Arabica',
                                    (np.where((df_Green['USD_per_TNE'].astype(float) < Q1_Ara) & 
                                        (df_Green['USD_per_TNE'].astype(float) >= Q1_Ro), 'Robusta','Outliers')))),
                            df_Green['VarietyFinal'])))))#TH3: Nguoc lai thi lay VarietyFinal
                        
                        print('6. Xác định Unit đối với các Bao/Kiện không có mô tả trọng lượng dựa trên phạm vi Outlier')
                        df_Green['UnitReCheck'] = np.where(pd.isna(df_Green['UnitCheck']),
                            (np.where((df_Green['VarietyFinal'] == 'Robusta') | (df_Green['VarietyFinal'] == ''),
                                (np.where((df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) >= Q1_Ro) & 
                                (df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) <= Q3_Ro),'tne',
                                    (np.where((df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) >= Q1_Ro * 0.06) & 
                                    (df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) <= Q3_Ro * 0.06),'60kg bag',
                                        (np.where((df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) >= Q1_Ro * 0.07) & 
                                        (df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) <= Q3_Ro * 0.07),'70kg bag','')))))),
                            (np.where((df_Green['VarietyFinal'] == 'Arabica'),
                                (np.where((df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) >= Q1_Ara) & 
                                (df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) <= Q3_Ara),'tne',
                                    (np.where((df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) >= Q1_Ara * 0.06) & 
                                    (df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) <= Q3_Ara * 0.06),'60kg bag',
                                        (np.where((df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) >= Q1_Ara * 0.07) & 
                                        (df_Green['invoice_amount'].astype(float)/df_Green['quantity'].astype(float) <= Q3_Ara * 0.07),'70kg bag','')))))),''))
                            )),'')
                        df_Green = pd.merge(df_Green, UnitRef, left_on='UnitReCheck', right_on='UnitCheck', how='left')#.drop(['result2'], axis=1)
                        df_Green.rename(columns={'UnitCheck_x':'UnitCheck','ConvertToMt_x':'ConvertToMt'}, inplace=True)
                        
                        print('6.1. Quy đổi toàn bộ số lượng sang TNE đối với các tờ khai có Unit là Bao/Kiện etc')
                        df_Green['UnitCheck'] = np.where(df_Green['UnitReCheck'] != '',df_Green['UnitCheck_y'], df_Green['UnitCheck'])
                        df_Green['ConvertToMt'] = np.where(df_Green['UnitReCheck'] != '',df_Green['ConvertToMt_y'], df_Green['ConvertToMt'])
                        df_Green['MtAdjusted'] = np.where(df_Green['UnitReCheck'] != '',df_Green['quantity'].astype(float) * df_Green['ConvertToMt_y'].astype(float), df_Green['MtAdjusted'])
                        
                        print('6.2. Tính giá USD/TNE đối với các tờ khai có Unit là Bao/Kiện etc')
                        df_Green['USD_per_TNE'] = np.where((df_Green['UnitReCheck'] != ''), (np.where(df_Green['invoice_currency']=='USD', 
                            df_Green['invoice_amount']/df_Green['MtAdjusted'].astype(float),
                            (np.where(df_Green['invoice_currency']=='VND', (df_Green['invoice_amount']/Avg_Fx_Rate)/df_Green['MtAdjusted'].astype(float), 
                                ((df_Green['invoice_amount']*df_Green['fx_rate'])/Avg_Fx_Rate)/df_Green['MtAdjusted'].astype(float))))),df_Green['USD_per_TNE'])

                        print(u'6.3. Phân loại Arabica/Robusta dựa trên Non-Outlier đối với các tờ khai có Unit là Bao/Kiện etc') #
                        df_Green['VarietyFinal'] = np.where(df_Green['UnitReCheck'] == '', df_Green['VarietyFinal'],# TH: Giu nguyen gia tri VarietyFinal neu UnitReCheck khong co gia tri
                            (np.where((df_Green['USD_per_TNE'] > 0) & (df_Green['MtAdjusted'] > 0), #TH mô tả không rõ R/A nhưng có Unit chuẩn
                                (np.where((df_Green['USD_per_TNE'].astype(float) >= Q1_Ro) & 
                                    (df_Green['USD_per_TNE'].astype(float) <= Q3_Ro), 'Robusta',
                                (np.where((df_Green['USD_per_TNE'].astype(float) > Q3_Ro) & 
                                    (df_Green['USD_per_TNE'].astype(float) < Q3_Ara), 'Arabica','Outliers')))),'')))
                        
                        print(u'7. Phân loại Arabica/Robusta một lần nữa đối với các tờ khai đã phân loại A/R qua Description') #
                        df_Green['VarietyFinal'] = np.where(df_Green['VarietyFromDescription'] == '', df_Green['VarietyFinal'],# TH1: da phan loai A/R tai field VarietyFromDescription
                            (np.where(df_Green['VarietyFromDescription'] == 'Robusta',
                                (np.where((df_Green['USD_per_TNE'] > 0) & (df_Green['MtAdjusted'] > 0), #TH mô tả không rõ R/A nhưng có Unit chuẩn
                                    (np.where((df_Green['USD_per_TNE'].astype(float) >= Q1_Ro) & 
                                        (df_Green['USD_per_TNE'].astype(float) <= Q3_Ro), 'Robusta',
                                    (np.where((df_Green['USD_per_TNE'].astype(float) > Q3_Ro) & 
                                        (df_Green['USD_per_TNE'].astype(float) < Q3_Ara), 'Arabica','Outliers')))),df_Green['VarietyFinal'])),
                            (np.where(df_Green['VarietyFromDescription'] == 'Arabica',
                                (np.where((df_Green['USD_per_TNE'] > 0) & (df_Green['MtAdjusted'] > 0), #TH mô tả không rõ R/A nhưng có Unit chuẩn
                                    (np.where((df_Green['USD_per_TNE'].astype(float) >= Q1_Ara) & 
                                        (df_Green['USD_per_TNE'].astype(float) <= Q3_Ara), 'Arabica',
                                    (np.where((df_Green['USD_per_TNE'].astype(float) < Q1_Ara) & 
                                        (df_Green['USD_per_TNE'].astype(float) >= Q1_Ro), 'Robusta','Outliers')))),df_Green['VarietyFinal'])), df_Green['VarietyFinal'])))))

                        print(u'8. Phân loại Outlier đối với các tờ khai có Unit là Bao/Kiện etc') #
                        df_Green['CheckOutlier'] = np.where(df_Green['UnitReCheck'] == '',df_Green['CheckOutlier'],
                            (np.where(df_Green['VarietyFinal'] == 'Outliers',df_Green['VarietyFinal'],
                                (np.where((df_Green['VarietyFinal']=='Robusta') & (df_Green['UnitCheck']!=''), #TH1: Xet Robusta va Units lay DonGia so voi Non-Outlier Ro de phan loai Outlier/A/R
                                    (np.where((df_Green['USD_per_TNE'].astype(float) >= Q1_Ro) & 
                                        (df_Green['USD_per_TNE'].astype(float) <= Q3_Ro), 'Robusta',
                                    (np.where((df_Green['USD_per_TNE'].astype(float) > Q3_Ro) & 
                                        (df_Green['USD_per_TNE'].astype(float) <= Q3_Ara), 'Arabica','Outliers')))),
                                    (np.where((df_Green['VarietyFinal']=='Arabica') & (df_Green['UnitCheck']!=''), #TH2: Xet Arabica va Units lay DonGia so voi Non-Outlier A de phan loai Outlier/A/R
                                        (np.where((df_Green['USD_per_TNE'].astype(float) >= Q1_Ara) & 
                                            (df_Green['USD_per_TNE'].astype(float) <= Q3_Ara), 'Arabica',
                                        (np.where((df_Green['USD_per_TNE'].astype(float) < Q1_Ara) & 
                                            (df_Green['USD_per_TNE'].astype(float) >= Q1_Ro), 'Robusta','Outliers')))),
                            df_Green['VarietyFinal'])))))))#TH3: Nguoc lai thi lay VarietyFinal

                        df_Green['VarietyFinal'] = np.where(df_Green['VarietyFromDescription'] != '',df_Green['VarietyFromDescription'],
                            (np.where(df_Green['VarietyFinal']=='Outlier','',df_Green['VarietyFinal'])))

                        df_Green = df_Green.drop(['UnitCheck_y', 'ConvertToMt_y'], axis=1)

                        df_Green['MtAdjusted'] = np.where((df_Green['VarietyFinal']=='Robusta') & (df_Green['MtAdjusted']==0), 
                            df_Green['USD_per_TNE'].astype(float)/IQR_R, (np.where((df_Green['VarietyFinal']=='Arabica') & (df_Green['MtAdjusted']==0), 
                                df_Green['USD_per_TNE'].astype(float)/IQR_A, df_Green['MtAdjusted'])))

                        # print('9. Quy doi invoice_amount sang VND')
                        # df_Green['Invoiceprice2'] = np.where(df_Green['fx_rate'] > 0, df_Green['invoice_amount'] / df_Green['quantity'] * df_Green['fx_rate'],
                        #     df_Green['invoice_amount'] / df_Green['quantity'])

                        print(u'9. Phân loại Outlier neu TNE > 1000') #
                        df_Green['TNE_Outlier'] = np.where((df_Green['MtAdjusted'].astype(float) > 1000),'Outlier', '')

                        print(u'10. Tách thành 2 file Outliers/Non-Outlier')
                        df_Outlier = df_Green[(df_Green['CheckOutlier']=='Outliers') | (df_Green['TNE_Outlier']=='Outliers')].reset_index(drop=True)
                        df_Green = df_Green[(df_Green['CheckOutlier']!='Outliers') & (df_Green['TNE_Outlier']=='')].reset_index().drop(['Unit_temp', 'CheckOutlier','UnitReCheck','TNE_Outlier'], axis=1)
                        df_Green['index_in_file'] = df_Green['index']
                        df_Green = df_Green.drop('index', axis=1)
                        print(df_Green)
                        df_Outlier.to_csv(path_save + file_str.split('.')[0] + '_Outlier.csv', index = None, header=True, encoding='utf-8-sig') #windows-1251
                        df_Green.to_csv(path_save + file_str.split('.')[0] + '_df_Green.csv', index = None, header=True, encoding='utf-8-sig') #windows-1251
                        # print(df_Green)
                        df_Green['Date'] = pd.to_datetime(df_Green['Date'])
                        # print('Trich xuat gia tri cac field can lay tu df_Green DataFrame (Cleaning Data)')
                        # df_cleaned_data = df_Green[['declaration_no', 'final_status_code', 'declaration_type_code', 'exporter_code', 'customs_clearance_place', 
                        # 'discharge_place_code', 'discharge_place_name', 'queue_place_code', 'queue_place_name', 'cash_code', 'tax_currency', 'taxable_value', 
                        # 'fx_rate', 'hs_code', 'product_description', 'HsDetail', 'NotCoffee', 'CategoryFinal', 'VarietyFinal', 'product_unit', 'UnitCheck', 'MtAdjusted', 'Date']]
                        # # print(df_cleaned_data)
                        # df_cleaned_data.to_csv(path_save + file_str.split('.')[0] + '_clean.csv', index = None, header=True, encoding='utf-8-sig') #windows-1251
                        print("--- %s seconds ---" % (time.time() - start_time))

                        print('Luu df_Green DataFrame (Cleaning Data) vao Database')
                        sql = "COPY nex.vn_imports_customs_cleaned (%s) FROM '%s' DELIMITER ',' CSV HEADER encoding 'UTF-8';" %(clean_cols_list, path_save + file_str.split('.')[0] + '_df_Green.csv')
                        print(sql)
                        with conn.cursor() as cur:
                            cur.execute(sql)
                            conn.commit()
                        with conn.cursor() as cur:
                            cur.execute("""DELETE FROM nex.vn_imports_customs_cleaned a USING nex.vn_imports_customs_cleaned b
                                        WHERE a.id < b.id AND ((a.declaration_no = b.declaration_no and a.file_date <> b.file_date) 
                                        or (a.declaration_no = b.declaration_no and a.index_in_file = b.index_in_file));""")
                            conn.commit()

                    except Exception as error:
                        print(error)
                        # send_email(os.path.basename(__file__), file_str, 'Can not processed data from VN Export Customes file')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()

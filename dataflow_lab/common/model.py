import csv
import logging

from apache_beam.io.gcp.internal.clients import bigquery
from datetime import datetime


class Offer(object):
    csv_header = 'offer_id,campaign_id,crm_product_id,sku,offer_name,display_txt,display_flag,offer_type_id,effective_from_date,effective_to_date,status,is_exported,exported_time,create_time,setting_time,create_by,setting_by,recommend,internet_code,need_synchronize,is_web_exported,limit_times,qty,special_price,what_reward_definition_id,times,pos_sku_for_sap,pos_sku_for_sap_time,calculate_in_report,is_dragon_exported,no_need_synchronize_dragon,available_country,external_id2,valid_online,valid_offline,valid_mobile_app,country_id'

    @staticmethod
    def get_bq_schema():

        def add_field(schema, field_name, field_type):
            field_schema = bigquery.TableFieldSchema()
            field_schema.name = field_name
            field_schema.type = field_type
            field_schema.mode = 'NULLABLE'
            schema.fields.append(field_schema)

        table_schema = bigquery.TableSchema()

        add_field(table_schema, 'offer_id', 'INTEGER')
        add_field(table_schema, 'campaign_id', 'INTEGER')
        add_field(table_schema, 'crm_product_id', 'INTEGER')
        add_field(table_schema, 'sku', 'STRING')
        add_field(table_schema, 'offer_name', 'STRING')
        add_field(table_schema, 'display_txt', 'STRING')
        add_field(table_schema, 'display_flag', 'INTEGER')
        add_field(table_schema, 'offer_type_id', 'INTEGER')
        add_field(table_schema, 'effective_from_date', 'TIMESTAMP')
        add_field(table_schema, 'effective_to_date', 'TIMESTAMP')
        add_field(table_schema, 'status', 'INTEGER')
        add_field(table_schema, 'is_exported', 'INTEGER')
        add_field(table_schema, 'exported_time', 'TIMESTAMP')
        add_field(table_schema, 'create_time', 'TIMESTAMP')
        add_field(table_schema, 'setting_time', 'TIMESTAMP')
        add_field(table_schema, 'create_by', 'INTEGER')
        add_field(table_schema, 'setting_by', 'INTEGER')
        add_field(table_schema, 'recommend', 'INTEGER')
        add_field(table_schema, 'internet_code', 'STRING')
        add_field(table_schema, 'need_synchronize', 'INTEGER')
        add_field(table_schema, 'is_web_exported', 'INTEGER')
        add_field(table_schema, 'limit_times', 'INTEGER')
        add_field(table_schema, 'qty', 'INTEGER')
        add_field(table_schema, 'special_price', 'FLOAT')
        add_field(table_schema, 'what_reward_definition_id', 'INTEGER')
        add_field(table_schema, 'times', 'INTEGER')
        add_field(table_schema, 'pos_sku_for_sap', 'STRING')
        add_field(table_schema, 'pos_sku_for_sap_time', 'TIMESTAMP')
        add_field(table_schema, 'calculate_in_report', 'INTEGER')
        add_field(table_schema, 'is_dragon_exported', 'INTEGER')
        add_field(table_schema, 'no_need_synchronize_dragon', 'INTEGER')
        add_field(table_schema, 'available_country', 'INTEGER')
        add_field(table_schema, 'external_id', 'INTEGER')
        add_field(table_schema, 'valid_online', 'INTEGER')
        add_field(table_schema, 'valid_offline', 'INTEGER')
        add_field(table_schema, 'valid_mobile_app', 'INTEGER')
        add_field(table_schema, 'country_id', 'INTEGER')

        return table_schema

    def __init__(self,
                 offer_id,
                 campaign_id,
                 crm_product_id,
                 sku,
                 offer_name,
                 display_txt,
                 display_flag,
                 offer_type_id,
                 effective_from_date,
                 effective_to_date, status=None, is_exported=None, exported_time=None, create_time=None,
                 setting_time=None, create_by=None, setting_by=None, recommend=None, internet_code=None,
                 need_synchronize=None, is_web_exported=None, limit_times=None, qty=None, special_price=None,
                 what_reward_definition_id=None, times=None, pos_sku_for_sap=None, pos_sku_for_sap_time=None,
                 calculate_in_report=None, is_dragon_exported=None, no_need_synchronize_dragon=None,
                 available_country=None, external_id2=None, valid_online=None, valid_offline=None,
                 valid_mobile_app=None, country_id=None):
        self.offer_id = offer_id
        self.campaign_id = campaign_id
        self.crm_product_id = crm_product_id
        self.sku = sku
        self.offer_name = offer_name
        self.display_txt = display_txt
        self.display_flag = display_flag
        self.offer_type_id = offer_type_id
        self.effective_from_date = Offer.parse_timestamp(effective_from_date)
        self.effective_to_date = Offer.parse_timestamp(effective_to_date)
        self.status = status
        self.is_exported = is_exported
        self.exported_time = Offer.parse_timestamp(exported_time)
        self.create_time = Offer.parse_timestamp(create_time)
        self.setting_time = Offer.parse_timestamp(setting_time)
        self.create_by = create_by
        self.setting_by = setting_by
        self.recommend = recommend
        self.internet_code = internet_code
        self.need_synchronize = need_synchronize
        self.is_web_exported = is_web_exported
        self.limit_times = limit_times
        self.qty = qty
        self.special_price = special_price
        self.what_reward_definition_id = what_reward_definition_id
        self.times = times
        self.pos_sku_for_sap = pos_sku_for_sap
        self.pos_sku_for_sap_time = Offer.parse_timestamp(pos_sku_for_sap_time)
        self.calculate_in_report = calculate_in_report
        self.is_dragon_exported = is_dragon_exported
        self.no_need_synchronize_dragon = no_need_synchronize_dragon
        self.available_country = available_country
        self.external_id = external_id2
        self.valid_online = valid_online
        self.valid_offline = valid_offline
        self.valid_mobile_app = valid_mobile_app
        self.country_id = country_id

    def __str__(self):
        return str(self.__dict__)

    @staticmethod
    def parse_timestamp(date_string):
        if ('' == date_string):
            return None
        return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S %Z')

    @staticmethod
    def parseFromCsvLine(line):
        logging.info("Parsing line: %s", line)
        if isinstance(line, bytes):
            line = line.encode('utf-8')
        reader = csv.DictReader([line], delimiter=',',
                                fieldnames=Offer.csv_header.split(','))
        line_dict = next(reader)
        return Offer(**dict(line_dict))

    def to_bq_row(self):
        dict = self.__dict__
        for key in dict:
            if dict[key]:
                value = dict[key]
                string_value = str(value)
                dict[key] = string_value
        return dict


if __name__ == '__main__':
    offer = Offer.parseFromCsvLine(
        u'1200006830,1200000533,524713,368269,Gold Monthly Complimentary Makeover,Gold Monthly Complimentary Makeover,1,2,2017-07-31 09:32:29 UTC,2100-12-31 23:59:59 UTC,1,0,,2017-07-31 17:37:53 UTC,2017-07-31 17:37:53 UTC,1,1,1,"",1,0,1,1,0,1200000658,0,"",,0,0,0,0,0,1,1,1,9')
    print(str(offer))

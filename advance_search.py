# -*- coding: utf-8 -*-
import re

def build_query(field, keyword,opt_index,query_standard):
    field = field.split(".flag")[0]
    if field != 'all':
        if opt_index != 2:
            # 非must_not的情况，高亮显示添加common解析器
            query_match = {
                "match":{"%s.common"%field:{"query":keyword,"analyzer": "english"}}
            }
        else:
            query_match = {
                "match": {field:keyword}
            }

    else:
        # 全字段匹配，multi_match
        if opt_index == 2:
            query_match = {
                "match":{"_all":keyword}
            }
        else:
            query_match = {
                "multi_match": {
                    "query": keyword,
                    "analyzer": "standard",
                    "type": "best_fields",
                    "fields": query_standard,
                    "tie_breaker": 0.3,
                    "minimum_should_match": "20%"
                }
            }

    return query_match


def init_query(opt_index):
    opts = ["must", "should", "must_not"]
    if opt_index == 2:
        return {"bool":{opts[opt_index]: [], 'should':[]}}
    else:
        return {'bool': {opts[opt_index]: []}}


def append_query(target_query, opt_index, build_query):
    opts = ["must", "should", "must_not"]
    target_query['bool'][opts[opt_index]].append(build_query)


def choose_opt(opt):
    opts = ['AND', 'OR', 'NOT']
    for i in range(len(opts)):
        if opt == opts[i]:
            return i
    return 0


def is_dict(items):
    return True if type(items) == dict else False

def query_to_dict(query):
    """
    query解析成字典
    return: dict, 解析成字典的形式
    """
    splt = re.split("( AND | OR | NOT )",query)
    stage_num = int((len(splt) - 1) / 2)
    dic = {}
    if stage_num == 0:
        term = re.split("[\(\)\[\]]", splt[0])
        term = [m for m in term if m != ""]
        dic["%s" % term[1]] = "%s" % term[0]
        return dic
    else:
        temp = dic
        for i in range(stage_num):
            opt_lct = -2 - 2 * i
            left = splt[:opt_lct]
            right = splt[opt_lct + 1:]
            opt = splt[opt_lct].split(" ")
            opt = [m for m in opt if m!=""][0]

            temp["stage_%d" % i] = {}
            cur_stage = temp["stage_%d" % i]
            cur_stage["opt"] = opt

            right_term = re.split("[\(\)\[\]]", right[0])
            right_term = [m for m in right_term if m != ""]
            cur_stage["%s.flag" % right_term[1]] = "%s" % right_term[0]
            # 加上.flag表示是右侧的

            if len(left) == 1:
                left_term = re.split("[\(\)\[\]]", left[0])
                left_term = [m for m in left_term if m != ""]
                cur_stage["%s" % left_term[1]] = "%s" % left_term[0]

            temp = cur_stage
        return dic["stage_0"]

def process_query(query,query_standard,highlight_fields=[]):
    opt = query.get('opt')
    if not opt:
        # # 一个查询字段的情况
        (field, val), = query.items()
        if field == "all":
            sub_query = {"bool": {"should": [{"multi_match": {
                    "query": val,
                    "analyzer": "standard",
                    "type": "best_fields",
                    "fields": query_standard,
                    "tie_breaker": 0.3,
                    "minimum_should_match": "20%"
                }}]}}
            highlight_fields.append("all")
            return sub_query, highlight_fields
        else:
            highlight_fields.append(field)
            sub_query = {"bool": {"should": [{"match": {"%s.common" % field: {"query": val, "analyzer": "english"}}}]}}
            return sub_query,highlight_fields

    # 0:must, 1:should, 2:must_not
    opt_index = choose_opt(opt)
    sub_query = init_query(opt_index)

    for key, value in query.items():
        if is_dict(value):
            if opt_index == 2:
                append_query(sub_query,1, process_query(value,query_standard,highlight_fields)[0])
            else:
                append_query(sub_query,opt_index, process_query(value,query_standard,highlight_fields)[0])
        elif value != opt:
            if opt_index == 2:
                if key.find(".flag")>=0:
                    # 右侧的写入must_not
                    append_query(sub_query,opt_index, build_query(key, value,opt_index,query_standard))
                else:
                    # 左侧写入should
                    highlight_fields.append(key)
                    append_query(sub_query, 1, build_query(key, value,1,query_standard))
            else:
                highlight_fields.append(key.split(".flag")[0])
                append_query(sub_query, opt_index, build_query(key, value,opt_index,query_standard))

    return sub_query,highlight_fields

def set_all_highlight(keys):
    highlight = {}
    for key in keys:
        field_list = key.split(':')
        if len(field_list) > 1:
            match_fields = []
            for i in range(1, len(field_list)):
                match_fields.append(field_list[i])

            highlight[field_list[0]] = {
                'matched_fields': match_fields,
                'type': 'fvh'}
        else:
            highlight[key] = {}
    return highlight

def set_advance_highlight(highlight_fields,all_fields):
    # highlight_fields:需要高亮的字段list
    # all_fields:全部高亮字段
    if "all" in highlight_fields:
        # 全字段高亮显示
        return set_all_highlight(all_fields)
    else:
        # 个别字段高亮显示
        highlight = {}
        for key in highlight_fields:
            highlight[key] = {'matched_fields': ["%s.common"%key],
                'type': 'fvh'}
        return highlight

# for test

query_standard = ['also_known_as',
                          'organism', 'organism.common',
                          'symbol',
                          'title',
                          'title.common']
highlight_all = ['accession', 'identifier', 'symbol', 'also_known_as',
     'organism:organism.common:organism.raw',
     'title:title.common:title.space']

str1 = "(human free)[all] NOT (free)[available ] AND (gene)[key] OR (man_8989)[gender] AND (young)[age]"
str1 = "(huasdf adsddf)[age]"
# str1 = "(huad hdf)[all] OR (ha_df)[age]"

str1 = "(human)[title] OR (SENGB)[accession] NOT (RNA)[molecule_type]"
# str1  ="(B)[Title] NOT (Journal Article)[Publication type] AND (human free)[all] NOT (free)[available ] AND (gene)[key] OR (man_8989)[gender] AND (young)[age]"
# str = "(CNGN_GENE5657612)[Gene ID] OR (PSMB5)[Symbol]"
str1 = "(human)[title] OR (homo)[organism]"
str1 = "(a)[A] AND (b)[B] NOT (c)[C] OR (d)[D] NOT (e)[E] NOT (f)[F] OR (g)[G] NOT (h)[H] NOT (j)[J] OR (k)[K] NOT (l)[L] NOT (m)[M] AND (n)[N]"
str1 = "(a)[A] OR (b)[all] NOT (c)[all] NOT (d)[D]"
# str1 = "(a)[all]"
dic = query_to_dict(str1)
sub_query,highlight_fields= process_query(dic,query_standard)
print(sub_query)
print(highlight_fields)
high = set_advance_highlight(highlight_fields,highlight_all)
print(high)
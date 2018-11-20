# -*- coding: utf-8 -*-

import re
import time
import logging

from .functions import get_cga_prefix, format_es_response
from .functions import settings

from advance_search import process_query,query_to_dict,set_advance_highlight


history_logger = logging.getLogger('search_history_logger')


def get_search_result_ori(query, q_start, q_size, db_name, es_index):
    """
    ES 查询逻辑
    :param query: 查询关键词
    :param q_start: 分页起始位置
    :param q_size: 分页大小
    :param db_name: 子库名称
    :param es_index: es 索引库
    :return: Json, 封装的查询结果
    """

    es_search = EsSearch(query, q_start, q_size, db_name, es_index)
    context = {'total': 0, 'result': [], 'took': None}
    start_time = time.time()

    es_search.set_filter()
    if db_name == 'literature':
        es_search.set_query(
            query_standard=['article:3', 'article.common:6',
                            'author:6', 'journal_search:0.5',
                            'keywords:1', 'keywords.common:7',
                            'publication_type:0.8'],

            query_whitespace=['accession:5', 'identifier:5',
                              'article.space:4', 'article.common:4',
                              'keywords.space:6', 'publication_type:1']
        )

        es_search.set_high_light(
            ['identifier', 'accession',
             'article:article.common:article.space',
             'author', 'journal_search',
             'publication_type', 'available',
             'keywords:keywords.common:keywords.space']
        )

        es_search.set_sort(sorts=None, scripts=['identifier', 'accession', 'article',
                                                'author', 'journal_search', 'publication_type',
                                                'available', 'keywords'])

    elif db_name == 'organism':
        es_search.set_query(
            query_standard=['organism.common:6',
                            'organism:2',
                            'other_names:0.6',
                            'other_names.common:3'],

            query_whitespace=['accession:5',
                              'identifier:5',
                              'organism.raw:3',
                              'literature_id_list:4',
                              'other_names.space:0.8']
        )

        es_search.set_high_light(
            ['organism:organism.common:organism.raw',
             'accession', 'identifier',
             'literature_id_list',
             'other_names:other_names.common:other_names.space']
        )

        es_search.set_sort(None)

    elif db_name == 'sequence':
        es_search.set_query(
            query_standard=['clone:0.5',
                            'gene_symbol_list:1', 'gene_symbol_list.common:3',
                            'keywords:0.6', 'keywords.common:1',
                            'organism:3', 'organism.common:6',
                            'product:0.5', 'reference_title:0.6', 'reference_title.common:1',
                            'tissue_type:0.5',
                            'title:3', 'title.common:5',
                            'xreference_search:1'],

            query_whitespace=['accession:5', 'identifier:5', 'keywords.space:0.8',
                              'organism.raw:5',
                              'reference_title.space:0.8',
                              'title.space:8','literature_id_list.common:1','sequence_id:5']
        )

        es_search.set_high_light(
            ['accession', 'identifier',
             'title:title.common:title.space',
             'organism:organism.common:organism.raw',
             'division', 'molecule_type',
             'gene_symbol_list:gene_symbol_list.common',
             'literature_id_list',
             'xreference_search']
        )

        es_search.set_sort(None)

    elif db_name == 'genome':
        es_search.set_query(
            query_standard=['description:2', 'description.common:4',
                            'organism.common:4', 'organism:3',
                            'lineage:1.2', 'lineage.common:8',
                            'title:4', 'title.common:8',
                            'submitter:0.8'],

            query_whitespace=['accession:5',
                              'identifier:5',
                              'description.space:3',
                              'organism.raw:5',
                              'organism_id:5',
                              'literature_id_list:4',
                              'lineage.space:3',
                              'other_project_id:5',
                              'project_id:5',
                              'title.space:5']
        )

        es_search.set_high_light(
            ['accession',
             'organism_id',
             'organism:organism.common:organism.raw',
             'literature_id_list',
             'other_project_id',
             'identifier', 'submitter',
             'title:title.common:title.space',
             'project_id',
             'description:description.common:description.space',
             'lineage:lineage.common:lineage.space']
        )

        es_search.set_sort(None)

    elif db_name == 'protein':
        es_search.set_query(
            query_standard=['entry_name:5', 'gene_symbol_list:0.4',
                            'gene_symbol_list.common:0.6', 'keywords:0.3',
                            'keywords.common:0.7', 'altnames:1',
                            'organism:2', 'organism.common:3', 'protein_names:2',
                            'protein_names.common:3', 'status:5'],

            query_whitespace=['identifier:6', 'organism_id:0.5', 'other_id:0.5',
                              'literature_id_list:1', 'accession:5',
                              'gene_symbol_list.space:1',
                              'keywords.space:1', 'organism.space:5',
                              'protein_names.space:5']
        )

        es_search.set_high_light(
            ['accession', 'protein_names:protein_names.common:protein_names.space',
             'altnames', 'identifier', 'entry_name',
             'organism:organism.common:organism.space',
             'gene_symbol_list:gene_symbol_list.common:gene_symbol_list.space',
             'status', 'keywords:keywords.common:keywords.space']
        )

        es_search.set_sort(['status:asc'])

    elif db_name == 'variant':
        es_search.set_query(
            query_standard=['condition_name_list:2', 'condition_name_list.common:5',
                            'gene_id_list:0.5', 'gene_symbol_list:0.2',
                            'gene_symbol_list.common:0.8', 'hgvs:5',
                            'organism:3', 'organism.common:8',
                            'phenotype_name_list.common:0.7'],

            query_whitespace=['accession:5', 'identifier:6', 'condition_id_list:0.5',
                              'organism_id:0.5', 'literature_id_list:0.5',
                              'phenotype_id_list:0.5',
                              'phenotype_name_list:0.3', 'phenotype_name_list.space:1',
                              'condition_name_list.space:6',
                              'gene_symbol_list.space:2', 'organism.space:0.2']
        )

        es_search.set_high_light(
            ['accession', 'hgvs', 'location',
             'organism:organism.common:organism.space',
             'gene_symbol_list:gene_symbol_list.common:gene_symbol_list.space',
             'condition_name_list:condition_name_list.common:condition_name_list.space',
             'phenotype_name_list:phenotype_name_list.common:phenotype_name_list.space',
             'literature_id_list', 'project_list', 'identifier']
        )

        es_search.set_sort(None)

    elif db_name == 'project':
        es_search.set(doc_type='project')
        es_search.set_query(
            query_standard=['data_type:0.9', 'title.common:1.5', 'title:1.2',
                            'description:1.5', 'description.common:2'],

            query_whitespace=['accession_id:3', 'accession_in_other_database:1.2',
                              'title.space:1.6', 'description.space:1.8']
        )

        es_search.set_high_light(
            ['accession_id', 'accession_in_other_database',
             'title:title.common:title.space',
             'description:description.common:description.space',
             'data_type']
        )

    elif db_name == 'sample':
        es_search.set(doc_type='sample')
        es_search.set_query(
            query_standard=['organism.common:1.5', 'sample_name:1.5', 'organism:1.2',
                            'sample_title:1.2', 'sample_type:1', 'description:1.5',
                            'description.common:1.5', 'sample_title.common:2'],

            query_whitespace=['accession_id:3', 'related_accession:2', 'tax_id:0.8',
                              'accession_in_other_database:1.2', 'sample_title.space:1.8',
                              'description.space:1.5', 'organism.space:1.5']
        )

        es_search.set_high_light(
            ['accession_id', 'related_accession', 'accession_in_other_database',
             'tax_id', 'sample_name', 'sample_type',
             'sample_title:sample_title.common:sample_title.space',
             'organism:organism.common:organism.space',
             'description:description.common:description.space']
        )

    elif db_name == 'experiment':
        es_search.set(doc_type='experiment')
        es_search.set_query(
            query_standard=['platform:1.2', 'strategy:1', 'source:0.8',
                            'title:1.5', 'selection:1'],

            query_whitespace=['accession_id:3', 'related_accession:2',
                              'accession_in_other_database:0.9',
                              'title.space:1.2']
        )

        es_search.set_high_light(
            ['accession_id', 'related_accession', 'accession_in_other_database',
             'title', 'platform', 'strategy',
             'source', 'selection']
        )

    elif db_name == 'run':
        es_search.set(doc_type='run')
        es_search.set_query(
            query_standard=['file_type:0.8'],

            query_whitespace=['accession_id:3', 'related_accession:2',
                              'accession_in_other_database:1.2', 'file_type:0.6']
        )

        es_search.set_high_light(
            ['accession_id', 'related_accession', 'accession_in_other_database', 'file_type']
        )

    elif db_name == 'assembly':
        es_search.set(doc_type='assembly')
        es_search.set_query(
            query_standard=['assembly_name:1.5', 'molecule_type:1',
                            'sequencing_technology:0.8', 'assembly_method:0.7'],

            query_whitespace=['accession_id:3', 'related_accession:2',
                              'accession_in_other_database:1.2',
                              'assembly_name:1', 'molecule_type:0.8', 'assembly_method:1']
        )

        es_search.set_high_light(
            ['accession_id', 'related_accession', 'accession_in_other_database',
             'assembly_name', 'molecule_type',
             'sequencing_technology', 'assembly_method']
        )

    elif db_name == 'gene':
        es_search.set_query(
            query_standard=['also_known_as:3',
                            'organism:0.6', 'organism.common:1.8',
                            'symbol:1',
                            'title:0.6',
                            'title.common:3'],

            query_whitespace=['accession:3', 'identifier:3',
                              'title.space:3', 'title.common:3',
                              'organism.raw:1',
                              'literature_id_list.common:0.5']
        )

        es_search.set_high_light(
            ['accession', 'identifier', 'symbol', 'also_known_as',
             'organism:organism.common:organism.raw',
             'title:title.common:title.space']
        )

        es_search.set_sort(None)

    context['result'], context['took'], context['total'] = es_search.execute(False)
    history_logger.info('db: {0}, query: {1}, response time: {2}'.format(db_name, query, time.time() - start_time))
    return context

def get_search_result(query, q_start, q_size, db_name, es_index):
    """
    ES 高级搜索查询逻辑
    :param query: 查询关键词，字符类型
    :param q_start: 分页起始位置
    :param q_size: 分页大小
    :param db_name: 子库名称
    :param es_index: es 索引库
    :return: Json, 封装的查询结果
    """
    es_search = EsSearch(query, q_start, q_size, db_name, es_index)
    context = {'total': 0, 'result': [], 'took': None}
    start_time = time.time()

    query_dict = query_to_dict(query)
    es_search.set_filter()

    if db_name == 'literature':
        query_standard = ['article', 'article.common',
                          'author', 'journal_search',
                          'keywords', 'keywords.common',
                          'publication_type']
        highlight_all = ['identifier', 'accession',
             'article:article.common:article.space',
             'author', 'journal_search',
             'publication_type', 'available',
             'keywords:keywords.common:keywords.space']
        es_search.set_sort(None)

    elif db_name == 'organism':
        query_standard = ['organism.common',
                          'organism',
                          'other_names',
                          'other_names.common']
        highlight_all= ['organism:organism.common:organism.raw',
             'accession', 'identifier',
             'literature_id_list',
             'other_names:other_names.common:other_names.space']
        es_search.set_sort(None)

    elif db_name == 'sequence':
        query_standard = ['clone',
                          'gene_symbol_list', 'gene_symbol_list.common',
                          'keywords', 'keywords.common',
                          'organism', 'organism.common',
                          'product', 'reference_title', 'reference_title.common',
                          'tissue_type',
                          'title', 'title.common',
                          'xreference_search']
        highlight_all = ['accession', 'identifier',
             'title:title.common:title.space',
             'organism:organism.common:organism.raw',
             'division', 'molecule_type',
             'gene_symbol_list:gene_symbol_list.common',
             'literature_id_list',
             'xreference_search']
        es_search.set_sort(None)

    elif db_name == 'genome':
        query_standard = ['description', 'description.common',
                          'organism.common', 'organism',
                          'lineage', 'lineage.common',
                          'title', 'title.common',
                          'submitter']
        highlight_all = ['accession',
             'organism_id',
             'organism:organism.common:organism.raw',
             'literature_id_list',
             'other_project_id',
             'identifier', 'submitter',
             'title:title.common:title.space',
             'project_id',
             'description:description.common:description.space',
             'lineage:lineage.common:lineage.space']
        es_search.set_sort(None)

    elif db_name == 'protein':
        query_standard = ['entry_name', 'gene_symbol_list',
                          'gene_symbol_list.common', 'keywords',
                          'keywords.common', 'altnames',
                          'organism', 'organism.common', 'protein_names',
                          'protein_names.common', 'status']
        highlight_all = ['accession', 'protein_names:protein_names.common:protein_names.space',
             'altnames', 'identifier', 'entry_name',
             'organism:organism.common:organism.space',
             'gene_symbol_list:gene_symbol_list.common:gene_symbol_list.space',
             'status', 'keywords:keywords.common:keywords.space']
        es_search.set_sort(None)
        # 注意这里asc表示的是升序的意思

    elif db_name == 'variant':
        query_standard = ['condition_name_list', 'condition_name_list.common',
                          'gene_id_list', 'gene_symbol_list',
                          'gene_symbol_list.common', 'hgvs',
                          'organism', 'organism.common',
                          'phenotype_name_list.common']
        highlight_all = ['accession', 'hgvs', 'location',
             'organism:organism.common:organism.space',
             'gene_symbol_list:gene_symbol_list.common:gene_symbol_list.space',
             'condition_name_list:condition_name_list.common:condition_name_list.space',
             'phenotype_name_list:phenotype_name_list.common:phenotype_name_list.space',
             'literature_id_list', 'project_list', 'identifier']
        es_search.set_sort(None)

    elif db_name == 'gene':
        query_standard = ['also_known_as',
                          'organism', 'organism.common',
                          'symbol',
                          'title',
                          'title.common']
        highlight_all = ['accession', 'identifier', 'symbol', 'also_known_as',
             'organism:organism.common:organism.raw',
             'title:title.common:title.space']
        es_search.set_sort(None)

    elif db_name == 'project':
        es_search.set(doc_type='project')
        es_search.set_query(
            query_standard=['data_type:0.9', 'title.common:1.5', 'title:1.2',
                            'description:1.5', 'description.common:2'],

            query_whitespace=['accession_id:3', 'accession_in_other_database:1.2',
                              'title.space:1.6', 'description.space:1.8']
        )

        es_search.set_high_light(
            ['accession_id', 'accession_in_other_database',
             'title:title.common:title.space',
             'description:description.common:description.space',
             'data_type']
        )

    elif db_name == 'sample':
        es_search.set(doc_type='sample')
        es_search.set_query(
            query_standard=['organism.common:1.5', 'sample_name:1.5', 'organism:1.2',
                            'sample_title:1.2', 'sample_type:1', 'description:1.5',
                            'description.common:1.5', 'sample_title.common:2'],

            query_whitespace=['accession_id:3', 'related_accession:2', 'tax_id:0.8',
                              'accession_in_other_database:1.2', 'sample_title.space:1.8',
                              'description.space:1.5', 'organism.space:1.5']
        )

        es_search.set_high_light(
            ['accession_id', 'related_accession', 'accession_in_other_database',
             'tax_id', 'sample_name', 'sample_type',
             'sample_title:sample_title.common:sample_title.space',
             'organism:organism.common:organism.space',
             'description:description.common:description.space']
        )

    elif db_name == 'experiment':
        es_search.set(doc_type='experiment')
        es_search.set_query(
            query_standard=['platform:1.2', 'strategy:1', 'source:0.8',
                            'title:1.5', 'selection:1'],

            query_whitespace=['accession_id:3', 'related_accession:2',
                              'accession_in_other_database:0.9',
                              'title.space:1.2']
        )

        es_search.set_high_light(
            ['accession_id', 'related_accession', 'accession_in_other_database',
             'title', 'platform', 'strategy',
             'source', 'selection']
        )

    elif db_name == 'run':
        es_search.set(doc_type='run')
        es_search.set_query(
            query_standard=['file_type:0.8'],

            query_whitespace=['accession_id:3', 'related_accession:2',
                              'accession_in_other_database:1.2', 'file_type:0.6']
        )

        es_search.set_high_light(
            ['accession_id', 'related_accession', 'accession_in_other_database', 'file_type']
        )

    elif db_name == 'assembly':
        es_search.set(doc_type='assembly')
        es_search.set_query(
            query_standard=['assembly_name:1.5', 'molecule_type:1',
                            'sequencing_technology:0.8', 'assembly_method:0.7'],

            query_whitespace=['accession_id:3', 'related_accession:2',
                              'accession_in_other_database:1.2',
                              'assembly_name:1', 'molecule_type:0.8', 'assembly_method:1']
        )

        es_search.set_high_light(
            ['accession_id', 'related_accession', 'accession_in_other_database',
             'assembly_name', 'molecule_type',
             'sequencing_technology', 'assembly_method']
        )

    sub_query, highlight_fields = process_query(query_dict,query_standard)
    es_search.advance_query = sub_query
    es_search.highlight = set_advance_highlight(highlight_fields,highlight_all)

    context['result'], context['took'], context['total'] = es_search.execute(True)
    history_logger.info('db: {0}, query: {1}, response time: {2}'.format(db_name, query, time.time() - start_time))
    return context

class EsSearch(object):
    def __init__(self, query, start, size, db_name, es_index, doc_type='data'):
        self.query = query
        self.es_index = es_index
        self.doc_type = doc_type
        self.start = start
        self.size = size
        self.db_name = db_name
        self.query_should = []#针对非高级搜索构建query_body用
        self.advance_query = {}# 针对高级检索构建query_body用
        self.filter = []
        self.highlight = {}
        self.sort = []
        self.only_filter = False

    def set(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):
                raise ValueError('Class {0} has no attribute: {1}'.format(self.__class__.__name__, k))
            else:
                setattr(self, k, v)

    def _choose_field_type(self):
        if self.query is None: return "best_fields"

        words = self.query.split('\\s+')
        return "best_fields" if len(words) > 1 else "most_fields"

    def _set_query_boost(self, query_field):
        field_with_boost = []
        for e in query_field:
            arr = e.split(':')
            var = arr[0] + '^' + str(arr[1])
            field_with_boost.append(var)
        return field_with_boost

    def set_query(self, query_standard=[], query_whitespace=[]):
        analyzers = ["english", "whitespace"]
        fields_type = self._choose_field_type()
        for index, query_field in enumerate([query_standard, query_whitespace]):
            field_with_boost = self._set_query_boost(query_field)
            query_match = {
                "multi_match": {
                    "query": self.query,
                    "analyzer": analyzers[index],
                    "type": fields_type,
                    "fields": field_with_boost,
                    "tie_breaker": 0.3,
                    "minimum_should_match": "20%"
                }
            }
            self.query_should.append(query_match)

    def set_filter(self):
        filter_patten = re.compile(
            r'\[(gene|genome|protein|literature|sequence|organism|variant|project|sample|experiment|run|'
            r'assembly):(\d+)\]')

        db_id_patten = (
            ('literature', re.compile(r'{0}(\d+)'.format(get_cga_prefix('literature')[0]))),
            ('organism', re.compile(r'{0}(\d+)'.format(get_cga_prefix('organism')[0]))),

        )

        source_db, source_id = (None, None)
        if filter_patten.search(self.query):
            # 仅过滤
            self.only_filter = True
            source_db, source_id = filter_patten.findall(self.query)[0]
        else:
            # 扩展过滤
            for db_name, db_re_patten in db_id_patten:
                if db_re_patten.search(self.query):
                    source_db = db_name
                    source_id = db_re_patten.findall(self.query)[0]
                    break

        if source_db is not None:
            if source_db == self.db_name:
                if self.only_filter:
                    filter_field = 'identifier'
                else:
                    filter_field = 'accession'
                    source_id = self.query

            elif source_db in ['gene', 'literature']:
                filter_field = source_db + '_id_list'
            elif source_db == 'organism':
                filter_field = 'organism_id'
            else:
                filter_field = 'identifier'

            self.filter.append(
                {'term': {filter_field: source_id}}
            )

    def set_high_light(self, keys):
        for key in keys:
            field_list = key.split(':')
            if len(field_list) > 1:
                match_fields = []
                for i in range(1, len(field_list)):
                    match_fields.append(field_list[i])

                self.highlight[field_list[0]] = {
                    'matched_fields': match_fields,
                    'type': 'fvh'}
            else:
                self.highlight[key] = {}

    def set_script(self, scripts):
        start = "def int count = 0;"
        tmp = start
        for e in scripts:
            name = "doc[\"" + e + "\"].value"
            tmp += "if (" + name + " != null) {count = count + 1};"
        tmp += "return count"
        return tmp

    def set_sort(self, sorts, scripts=None, priority=True):
        default = {"_score": {"order": "desc"}}
        if sorts is None:
            self.sort.append(default)
            return

        custom = {}
        for item in sorts:
            array = item.split(':')
            if len(array) > 1:
                custom[array[0]] = {"order": array[1]}
            else:  # 默认为降序
                custom[array[0]] = {"order": "desc"}

        script = None
        if scripts is not None:
            script = {"_script": {
                "script": {
                    "inline": self.set_script(scripts)
                },
                "type": "number",
                "order": "desc"}
            }

        if priority:  # 默认分数优先
            self.sort.append(default)
            if script is not None:
                self.sort.append(script)
            self.sort.append(custom)
        else:         # 以自定义优先
            self.sort.append(custom)
            if script is not None:
                self.sort.append(script)
            self.sort.append(default)

    def execute(self,advance_flag):
        # TODO: fragment_size 大小会影响高亮效果，需要详细调研
        q_body = {
            'size': self.size,
            'from': self.start,
            'min_score': 1e-6,
            'query': {'bool': {}},
            'highlight': {
                'pre_tags': ["<span class='high-light'>"],
                'post_tags': ["</span>"],
                'fragment_size': 1024,
                'fields': self.highlight
            }
        }

        # 如果符合过滤规则，优先进行过滤。暂不支持过滤 + 查询。
        if self.filter:
            del q_body['min_score']
            q_body['query']['bool']['filter'] = self.filter
            if self.db_name not in ['project', 'sample', 'experiment', 'run', 'assembly']:
                q_body['sort'] = self.sort
        else:
            if not advance_flag:
                q_body['query']['bool']['should'] = self.query_should
            else:
                # 高级检索的情况
                q_body['query'] =  self.advance_query

        if self.db_name in ['project', 'sample', 'experiment', 'run', 'assembly']:
            client = settings.ES_CNSA
        else:
            client = settings.ES

        # 其中batched_reduce_size参数对版本要求较高，默认为512, 在6.3/5.6版本以上中可以使用
        res = client.search(index=self.es_index, doc_type=self.doc_type, body=q_body,
                            batched_reduce_size=1024, timeout='15s')
        formatted_res = format_es_response(self.db_name, res.get('hits').get('hits'))

        return formatted_res, res.get('took'), res.get('hits').get('total')


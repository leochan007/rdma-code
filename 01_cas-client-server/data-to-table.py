#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xlwt

TABLE_NAME = 'data-cas-sequential'

def write_excel(path):
    workbook = xlwt.Workbook(encoding='utf-8')

    data_sheet = workbook.add_sheet('demo')

    row0 = ['hello', 'world']

    for i in range(len(row0)):
        data_sheet.write(0, i, row0[i])

    workbook.save(path)

if __name__ == '__main__':
    path = './' + TABLE_NAME + '.xls'
    write_excel(path)
    print('create table successfully!')
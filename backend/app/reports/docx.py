"""Streaming OOXML report builder: all records, without a giant document tree."""
import json
import re
from xml.sax.saxutils import escape
from zipfile import ZipFile, ZIP_DEFLATED

W = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
INVALID_XML = re.compile('[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]')

def text(value: str) -> str:
    # JSON representation retains control characters as escapes before XML encoding.
    return escape(INVALID_XML.sub(lambda m:'\\u%04x'%ord(m[0]),value))

class DocxWriter:
    def __init__(self,path: str,metadata: dict) -> None:
        self.archive = ZipFile(path,'w',ZIP_DEFLATED,allowZip64=True)
        self.archive.writestr('[Content_Types].xml','''<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/><Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/></Types>''')
        self.archive.writestr('_rels/.rels','''<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>''')
        self.archive.writestr('word/_rels/document.xml.rels','''<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>''')
        self.archive.writestr('word/styles.xml',f'''<w:styles xmlns:w="{W}">
<w:docDefaults><w:rPrDefault><w:rPr><w:rFonts w:ascii="Arial" w:hAnsi="Arial" w:cs="Arial"/><w:sz w:val="22"/><w:color w:val="000000"/></w:rPr></w:rPrDefault><w:pPrDefault><w:pPr><w:spacing w:after="100" w:line="260" w:lineRule="auto"/></w:pPr></w:pPrDefault></w:docDefaults>
<w:style w:type="paragraph" w:default="1" w:styleId="Normal"><w:name w:val="Normal"/></w:style>
<w:style w:type="paragraph" w:styleId="Title"><w:name w:val="Title"/><w:basedOn w:val="Normal"/><w:pPr><w:keepNext/><w:spacing w:after="220"/></w:pPr><w:rPr><w:b/><w:sz w:val="36"/><w:color w:val="000000"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Heading1"><w:name w:val="heading 1"/><w:basedOn w:val="Normal"/><w:pPr><w:keepNext/><w:spacing w:before="240" w:after="140"/><w:outlineLvl w:val="0"/></w:pPr><w:rPr><w:b/><w:sz w:val="28"/><w:color w:val="000000"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Data"><w:name w:val="Data"/><w:basedOn w:val="Normal"/><w:pPr><w:spacing w:after="60" w:line="220" w:lineRule="auto"/></w:pPr><w:rPr><w:rFonts w:ascii="Courier New" w:hAnsi="Courier New"/><w:sz w:val="18"/></w:rPr></w:style></w:styles>''')
        self.output = self.archive.open('word/document.xml','w',force_zip64=True)
        self.write(f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><w:document xmlns:w="{W}"><w:body>')
        self.paragraph('Отчет нагрузочного теста MySQL','Title')
        self.paragraph('Полный отчет с параметрами теста, итоговой статистикой, всеми сохраненными показателями, операциями и системными событиями.')
        self.paragraph('Сессия '+metadata['session_id'])
        self.paragraph('Начало '+str(metadata['start_time'])+'    Завершение '+str(metadata['end_time'] or 'Тест продолжается'))
        counts=metadata['export']['counts']
        self.paragraph(f"Операций {counts['requests']}    Ошибок {counts['errors']}    Точек истории {counts['samples']}    Событий {counts['events']}")
        self.paragraph('Экспорт содержит все записи, сохраненные на '+metadata['export']['exported_at']+'. Выполняющиеся запросы и будущие события активного теста в этот снимок не входят.')
        for title,key in [('Параметры теста','config'),('Итоговая статистика','summary'),('Сведения об экспорте','export')]:
            self.section(title)
            self.value(metadata[key])

    def write(self,value: str) -> None:
        self.output.write(value.encode('utf-8'))

    def paragraph(self,value: str,style: str='Normal') -> None:
        self.write(f'<w:p><w:pPr><w:pStyle w:val="{style}"/></w:pPr><w:r><w:t xml:space="preserve">{text(value)}</w:t></w:r></w:p>')

    def section(self,title: str) -> None:
        self.paragraph(title,'Heading1')

    def value(self,value: object) -> None:
        # One field per paragraph lets Word paginate without oversized table rows.
        if isinstance(value,dict):
            for key,item in value.items():
                self.paragraph(key+' = '+json.dumps(item,ensure_ascii=False,separators=(',',':')),'Data')
        else:
            self.paragraph(json.dumps(value,ensure_ascii=False),'Data')

    def records(self,records: list[dict]) -> None:
        for record in records:
            self.paragraph(json.dumps(record,ensure_ascii=False,separators=(',',':')),'Data')

    def close(self) -> None:
        self.write('<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="1134" w:right="1134" w:bottom="1134" w:left="1134"/></w:sectPr></w:body></w:document>')
        self.abort()

    def abort(self) -> None:
        self.output.close()
        self.archive.close()

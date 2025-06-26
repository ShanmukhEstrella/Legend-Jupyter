import { JupyterFrontEnd, JupyterFrontEndPlugin } from '@jupyterlab/application';
import { IEditorLanguageRegistry } from '@jupyterlab/codemirror';
import { LRLanguage, LanguageSupport } from '@codemirror/language';
import { styleTags, tags as t } from '@lezer/highlight';

// Assume this was generated using: npx lezer-generator src/legend.grammar -o src/legend.grammar.js
import { parser } from './legend.grammar.js';

// Define the language using the Lezer parser
const LegendLanguage = LRLanguage.define({
  parser: parser.configure({
    props: [
      styleTags({
        Keyword: t.keyword,
        Number: t.number,
        Operator: t.operator,
        Identifier: t.variableName,
        LineComment: t.lineComment,
        "( )": t.paren
      })
    ]
  }),
  languageData: {
    commentTokens: { line: '#' }
  }
});

// Factory to wrap in LanguageSupport
function legendSupport() {
  return new LanguageSupport(LegendLanguage);
}

// JupyterLab plugin registration
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'legend-language-plugin',
  autoStart: true,
  requires: [IEditorLanguageRegistry],
  activate: (app: JupyterFrontEnd, languages: IEditorLanguageRegistry) => {
    languages.addLanguage({
      name: 'legend',
      mime: 'text/x-legend',
      extensions: ['.lgd'],
      load: async () => legendSupport()
    });
  }
};

export default plugin;

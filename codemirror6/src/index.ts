import { JupyterFrontEnd, JupyterFrontEndPlugin } from '@jupyterlab/application';
import { IEditorLanguageRegistry } from '@jupyterlab/codemirror';
import {
  LRLanguage,
  LanguageSupport,
  HighlightStyle,
  syntaxHighlighting
} from '@codemirror/language';
import { Tag, styleTags, tags as t } from '@lezer/highlight';

// Import the generated Lezer parser
import { parser } from './legend.grammar.js';

// Define a custom tag for the `NewOne` token
const myNewTag1: Tag = Tag.define();
const myNewTag2: Tag = Tag.define();

// Define the language with style tag mapping
const LegendLanguage = LRLanguage.define({
  parser: parser.configure({
    props: [
      styleTags({
        Keyword: t.keyword,
        Number: t.number,
        NewOne1: myNewTag1,
        NewOne2: myNewTag2,
        Operator: t.operator,
        Identifier: t.variableName,
        LineComment: t.lineComment,
        Parens: t.paren
      })
    ]
  }),
  languageData: {
    commentTokens: { line: '#' }
  }
});

//  Define custom styles, including your new tag
const legendHighlightStyle = HighlightStyle.define([
  { tag: myNewTag1, color: '#023961', fontWeight: 'bold' },
  { tag: myNewTag2, color: '#964B00', fontWeight: 'bold' }     
]);

// Language support bundle
function legendSupport(): LanguageSupport {
  return new LanguageSupport(LegendLanguage, [
    syntaxHighlighting(legendHighlightStyle)
  ]);
}

//  Register the plugin with JupyterLab
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

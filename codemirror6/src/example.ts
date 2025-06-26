import { parser } from './legend.grammar.js';
import {
  LRLanguage,
  LanguageSupport
} from '@codemirror/language';
import { styleTags, tags as t } from '@lezer/highlight';

export const legendLanguage = LRLanguage.define({
  parser: parser.configure({
    props: [
      styleTags({
        Keyword: t.keyword,
        Number: t.number,
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

export function legend(): LanguageSupport {
  return new LanguageSupport(legendLanguage);
}

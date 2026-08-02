import { describe, expect, test } from 'bun:test'
import { parseSchemaId, parseTaggedSearchId } from '../src/realBackend'

describe('tag-create readback verifier', () => {
  test('extracts only an explicit schema ID line', () => {
    expect(parseSchemaId('Supertag: public-prediction\nID: abc_123-Z\nOwn Fields (0):\n')).toBe('abc_123-Z')
    expect(parseSchemaId('created something but no schema readback')).toBeNull()
  })

  test('fallback selects the exact node carrying the supertag, not a same-name placeholder', () => {
    const output = JSON.stringify([
      { id: 'placeholder', name: 'public-prediction', tags: '' },
      { id: 'real-tag', name: 'public-prediction', tags: 'supertag' },
      { id: 'wrong-name', name: 'public-prediction-old', tags: 'supertag' },
    ])
    expect(parseTaggedSearchId(output, 'public-prediction')).toBe('real-tag')
    expect(parseTaggedSearchId('not-json', 'public-prediction')).toBeNull()
  })
})

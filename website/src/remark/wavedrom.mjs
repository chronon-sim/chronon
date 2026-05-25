import { visit } from 'unist-util-visit';

export default function remarkWaveDrom() {
  return (tree) => {
    visit(tree, 'code', (node, index, parent) => {
      if (node.lang !== 'wavedrom') return;

      parent.children.splice(index, 1, {
        type: 'mdxJsxFlowElement',
        name: 'WaveDrom',
        attributes: [
          {
            type: 'mdxJsxAttribute',
            name: 'value',
            value: node.value,
          },
        ],
        children: [],
      });
    });
  };
}

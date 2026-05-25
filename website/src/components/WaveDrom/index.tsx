import React, { useMemo } from 'react';

// @ts-expect-error wavedrom has no type declarations
import wavedrom from 'wavedrom';
// @ts-expect-error wavedrom has no type declarations
import waveSkin from 'wavedrom/skins/default';

let counter = 0;

export default function WaveDrom({ value }: { value: string }): React.ReactElement {
  const svgHtml = useMemo(() => {
    try {
      const parsed = JSON.parse(value);
      const jsonml = wavedrom.renderAny(counter++, parsed, waveSkin);
      return wavedrom.onml.stringify(jsonml);
    } catch (err) {
      return `<pre style="color:red">WaveDrom Error: ${String(err)}</pre>`;
    }
  }, [value]);

  return (
    <div
      className="wavedrom-container"
      dangerouslySetInnerHTML={{ __html: svgHtml }}
    />
  );
}

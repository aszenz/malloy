import script from './static/image.malloy?raw';
import {renderMalloy} from './render-malloy';

export default {
  title: 'Malloy/Image',
  render: ({source, view}, {globals: {connection}}) => {
    return renderMalloy({script, source, view, connection});
  },
  argTypes: {},
};

export const ImageAltFromRecord = {
  args: {
    source: 'logos',
    view: 'img_from_record',
  },
};

export const ImageAltFromParent = {
  args: {
    source: 'logos',
    view: 'img_from_parent',
  },
};

export const ImageAltFromGrandparent = {
  args: {
    source: 'logos',
    view: 'img_from_grandparent',
  },
};

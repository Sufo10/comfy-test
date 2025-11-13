import { app } from '/scripts/app.js';

app.registerExtension({
  name: 'Comfy.DynamicWorkflowEditor',
  async beforeRegisterNodeDef(nodeType, nodeData, app) {
    if (nodeData.name !== 'DynamicWorkflowEditorNode') return;

    const originalNodeCreated = nodeType.prototype.onNodeCreated;

    nodeType.prototype.onNodeCreated = async function () {
      if (originalNodeCreated) {
        originalNodeCreated.apply(this, arguments);
      }

      // Find the workflow_path widget
      const workflowPathWidget = this.widgets.find((w) => w.name === 'workflow_path');

      // Add a Refresh Inputs button
      const refreshButtonWidget = this.addWidget('button', '📝 Refresh Inputs');

      const fetchWorkflowJSON = async (path) => {
        if (!path) throw new Error('No path provided');

        // Call backend API to read JSON from path
        const response = await fetch(path, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path }),
        });

        if (!response.ok) {
          throw new Error(`Failed to load JSON: ${response.statusText}`);
        }

        const workflowJSON = await response.json();
        return workflowJSON;
      };

      const updateDynamicInputs = async () => {
        if (!workflowPathWidget || !workflowPathWidget.value) {
          alert('Please provide a valid workflow path.');
          return;
        }

        refreshButtonWidget.name = '⏳ Loading...';

        let workflowJSON;
        try {
          workflowJSON = await fetchWorkflowJSON(workflowPathWidget.value);
        } catch (error) {
          console.error('Error loading workflow JSON:', error);
          alert('Error loading workflow JSON. See console.');
          refreshButtonWidget.name = '📝 Refresh Inputs';
          return;
        }

        // Clear previous dynamic inputs
        this.widgets = this.widgets.filter((w) => !w.name.startsWith('dyn_'));
        this.inputs = this.inputs.filter((i) => !i.name.startsWith('dyn_'));

        // Create dynamic widgets for each primitive field
        for (const nodeId in workflowJSON) {
          const node = workflowJSON[nodeId];
          const inputs = node.inputs || {};
          for (const key in inputs) {
            const value = inputs[key];
            if (['string', 'number', 'boolean'].includes(typeof value)) {
              const widgetName = `dyn_${nodeId}_${key}`;
              let type = 'STRING';
              if (typeof value === 'number') type = Number.isInteger(value) ? 'INT' : 'FLOAT';
              if (typeof value === 'boolean') type = 'BOOLEAN';

              this.addWidget(type, widgetName, value);
            }
          }
        }

        refreshButtonWidget.name = '📝 Refresh Inputs';
        this.setDirtyCanvas(true);
        console.debug('Dynamic inputs updated:', this.widgets);
      };

      // Attach callback
      refreshButtonWidget.callback = updateDynamicInputs;
    };
  },
});

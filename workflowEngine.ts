/**
 * AI Workflow Engine - Core execution engine for automating AI-powered workflows
 * Supports multi-step pipelines, LLM integrations, API calls, and data transformations
 */

// Types and Interfaces
export interface WorkflowNode {
  id: string;
  type: 'llm' | 'api' | 'transform' | 'condition' | 'webhook';
  name: string;
  config: Record<string, any>;
  inputs: string[];
  outputs: string[];
}

export interface Workflow {
  id: string;
  name: string;
  description: string;
  nodes: WorkflowNode[];
  connections: Connection[];
  triggers: Trigger[];
  status: 'draft' | 'active' | 'paused';
}

export interface Connection {
  id: string;
  source: string;
  target: string;
  sourceOutput: string;
  targetInput: string;
}

export interface Trigger {
  type: 'manual' | 'webhook' | 'schedule' | 'event';
  config: Record<string, any>;
}

export interface ExecutionContext {
  workflowId: string;
  executionId: string;
  variables: Record<string, any>;
  nodeOutputs: Map<string, any>;
  startTime: Date;
  status: 'running' | 'completed' | 'failed' | 'paused';
}

// Workflow Engine Class
export class WorkflowEngine {
  private workflows: Map<string, Workflow>;
  private executions: Map<string, ExecutionContext>;
  
  constructor() {
    this.workflows = new Map();
    this.executions = new Map();
  }

  /**
   * Register a new workflow
   */
  registerWorkflow(workflow: Workflow): void {
    this.validateWorkflow(workflow);
    this.workflows.set(workflow.id, workflow);
    console.log(`Workflow registered: ${workflow.name} (${workflow.id})`);
  }

  /**
   * Execute a workflow
   */
  async executeWorkflow(
    workflowId: string,
    input: Record<string, any> = {}
  ): Promise<ExecutionContext> {
    const workflow = this.workflows.get(workflowId);
    
    if (!workflow) {
      throw new Error(`Workflow not found: ${workflowId}`);
    }

    if (workflow.status !== 'active') {
      throw new Error(`Workflow is not active: ${workflow.status}`);
    }

    const executionId = this.generateExecutionId();
    const context: ExecutionContext = {
      workflowId,
      executionId,
      variables: { ...input },
      nodeOutputs: new Map(),
      startTime: new Date(),
      status: 'running'
    };

    this.executions.set(executionId, context);

    try {
      await this.executeNodes(workflow, context);
      context.status = 'completed';
    } catch (error) {
      context.status = 'failed';
      console.error(`Workflow execution failed: ${error}`);
      throw error;
    }

    return context;
  }

  /**
   * Execute workflow nodes in topological order
   */
  private async executeNodes(
    workflow: Workflow,
    context: ExecutionContext
  ): Promise<void> {
    const sortedNodes = this.topologicalSort(workflow);
    
    for (const node of sortedNodes) {
      const inputs = this.collectNodeInputs(node, context, workflow);
      const output = await this.executeNode(node, inputs, context);
      context.nodeOutputs.set(node.id, output);
      
      console.log(`Node executed: ${node.name} (${node.type})`);
    }
  }

  /**
   * Execute a single node
   */
  private async executeNode(
    node: WorkflowNode,
    inputs: Record<string, any>,
    context: ExecutionContext
  ): Promise<any> {
    switch (node.type) {
      case 'llm':
        return await this.executeLLMNode(node, inputs);
      case 'api':
        return await this.executeAPINode(node, inputs);
      case 'transform':
        return this.executeTransformNode(node, inputs);
      case 'condition':
        return this.executeConditionNode(node, inputs);
      case 'webhook':
        return await this.executeWebhookNode(node, inputs);
      default:
        throw new Error(`Unknown node type: ${node.type}`);
    }
  }

  /**
   * Execute LLM node (OpenAI, Claude, Gemini, etc.)
   */
  private async executeLLMNode(
    node: WorkflowNode,
    inputs: Record<string, any>
  ): Promise<any> {
    const { provider, model, prompt, temperature = 0.7 } = node.config;
    const processedPrompt = this.interpolateVariables(prompt, inputs);

    // Simulate LLM API call
    console.log(`Calling ${provider} ${model} with prompt: ${processedPrompt}`);
    
    return {
      response: `AI Response from ${provider} ${model}`,
      usage: { promptTokens: 100, completionTokens: 50, totalTokens: 150 },
      model,
      provider
    };
  }

  /**
   * Execute API call node
   */
  private async executeAPINode(
    node: WorkflowNode,
    inputs: Record<string, any>
  ): Promise<any> {
    const { url, method = 'GET', headers = {}, body } = node.config;
    const processedUrl = this.interpolateVariables(url, inputs);
    const processedBody = body ? this.interpolateVariables(JSON.stringify(body), inputs) : null;

    console.log(`API Call: ${method} ${processedUrl}`);
    
    // Simulate API response
    return {
      status: 200,
      data: { message: 'API call successful', timestamp: new Date() }
    };
  }

  /**
   * Execute data transformation node
   */
  private executeTransformNode(
    node: WorkflowNode,
    inputs: Record<string, any>
  ): any {
    const { operation, field, value } = node.config;

    switch (operation) {
      case 'map':
        return inputs[field]?.map((item: any) => ({
          ...item,
          transformed: true
        }));
      case 'filter':
        return inputs[field]?.filter((item: any) => 
          this.evaluateCondition(item, value)
        );
      case 'extract':
        return inputs[field];
      case 'merge':
        return { ...inputs, merged: true };
      default:
        return inputs;
    }
  }

  /**
   * Execute conditional logic node
   */
  private executeConditionNode(
    node: WorkflowNode,
    inputs: Record<string, any>
  ): any {
    const { condition, truePath, falsePath } = node.config;
    const result = this.evaluateCondition(inputs, condition);
    
    return {
      conditionMet: result,
      nextPath: result ? truePath : falsePath
    };
  }

  /**
   * Execute webhook node
   */
  private async executeWebhookNode(
    node: WorkflowNode,
    inputs: Record<string, any>
  ): Promise<any> {
    const { url, method = 'POST', headers = {} } = node.config;
    
    console.log(`Webhook: ${method} ${url}`);
    
    return {
      status: 200,
      delivered: true,
      timestamp: new Date()
    };
  }

  /**
   * Collect inputs for a node from previous node outputs
   */
  private collectNodeInputs(
    node: WorkflowNode,
    context: ExecutionContext,
    workflow: Workflow
  ): Record<string, any> {
    const inputs: Record<string, any> = {};
    
    const incomingConnections = workflow.connections.filter(
      conn => conn.target === node.id
    );

    for (const connection of incomingConnections) {
      const sourceOutput = context.nodeOutputs.get(connection.source);
      if (sourceOutput) {
        inputs[connection.targetInput] = sourceOutput[connection.sourceOutput] || sourceOutput;
      }
    }

    return { ...context.variables, ...inputs };
  }

  /**
   * Topological sort for workflow execution order
   */
  private topologicalSort(workflow: Workflow): WorkflowNode[] {
    const sorted: WorkflowNode[] = [];
    const visited = new Set<string>();
    const inDegree = new Map<string, number>();

    // Calculate in-degrees
    workflow.nodes.forEach(node => inDegree.set(node.id, 0));
    workflow.connections.forEach(conn => {
      inDegree.set(conn.target, (inDegree.get(conn.target) || 0) + 1);
    });

    // Find nodes with no incoming edges
    const queue: WorkflowNode[] = workflow.nodes.filter(
      node => inDegree.get(node.id) === 0
    );

    while (queue.length > 0) {
      const node = queue.shift()!;
      sorted.push(node);
      visited.add(node.id);

      // Update connected nodes
      workflow.connections
        .filter(conn => conn.source === node.id)
        .forEach(conn => {
          const targetDegree = (inDegree.get(conn.target) || 0) - 1;
          inDegree.set(conn.target, targetDegree);
          
          if (targetDegree === 0) {
            const targetNode = workflow.nodes.find(n => n.id === conn.target);
            if (targetNode) queue.push(targetNode);
          }
        });
    }

    if (sorted.length !== workflow.nodes.length) {
      throw new Error('Workflow contains cycles');
    }

    return sorted;
  }

  /**
   * Validate workflow structure
   */
  private validateWorkflow(workflow: Workflow): void {
    if (!workflow.id || !workflow.name) {
      throw new Error('Workflow must have id and name');
    }

    if (workflow.nodes.length === 0) {
      throw new Error('Workflow must have at least one node');
    }

    // Check for valid connections
    const nodeIds = new Set(workflow.nodes.map(n => n.id));
    for (const conn of workflow.connections) {
      if (!nodeIds.has(conn.source) || !nodeIds.has(conn.target)) {
        throw new Error(`Invalid connection: ${conn.id}`);
      }
    }
  }

  /**
   * Interpolate variables in strings
   */
  private interpolateVariables(
    template: string,
    variables: Record<string, any>
  ): string {
    return template.replace(/\{\{(\w+)\}\}/g, (_, key) => 
      variables[key] !== undefined ? String(variables[key]) : `{{${key}}}`
    );
  }

  /**
   * Evaluate condition expressions
   */
  private evaluateCondition(data: any, condition: string): boolean {
    // Simple condition evaluation (can be extended)
    try {
      return eval(this.interpolateVariables(condition, data));
    } catch {
      return false;
    }
  }

  /**
   * Generate unique execution ID
   */
  private generateExecutionId(): string {
    return `exec_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Get workflow status
   */
  getWorkflow(workflowId: string): Workflow | undefined {
    return this.workflows.get(workflowId);
  }

  /**
   * Get execution context
   */
  getExecution(executionId: string): ExecutionContext | undefined {
    return this.executions.get(executionId);
  }

  /**
   * List all workflows
   */
  listWorkflows(): Workflow[] {
    return Array.from(this.workflows.values());
  }
}

export default WorkflowEngine;

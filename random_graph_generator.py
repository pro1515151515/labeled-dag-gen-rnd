'''
@File        :Random_Graph_Generator.py
@Description :Batch generate workflow shapes and workload.
Workflow shape is save in mermaid graph, which can open by many
markdown viewer. https://draw.io can generate editable graph by 
mermaid text and export to svg, no need to draw graph by python.
@Date        :2024/01/14 21:59:59
@Author      :QianCheng
@Version     :1.0
'''
import random
import re
import os
path_this = os.path.dirname(os.path.abspath('__file__'))
def generate_random_graph(
        save_name="edge_workflows",
        random_seed=1234,
        workflow_numbers=100,
        parallelism=4, 
        layer_num_min=5, 
        layer_num_max=7, 
        connect_prob=0.5, 
        min_workload=0.5, 
        max_workload=30.0
    ):
    random.seed(random_seed)
    path_save = f"{path_this}/{save_name}.md"
    with open(path_save,"w",encoding='utf-8') as f:
        for workflow_id in range(workflow_numbers):
            ranks=[0] # ranks[node_index] = node's rank (i.e. distance from root)
            edges=[] # [[source_node_index, target_node_index],...]
            # remove root layer and sink layer
            layer_num = random.randint(layer_num_min - 2, layer_num_max - 2) 
            last_layer_from_index=0
            last_layer_to_index=1
            for rank in range(1,layer_num+1):
                task_num_of_this_layer = random.randint(1,parallelism)
                for _ in range(task_num_of_this_layer):
                    ranks.append(rank)
                this_layer_from_index = last_layer_to_index
                this_layer_to_index = this_layer_from_index + task_num_of_this_layer
                for i in range(this_layer_from_index, this_layer_to_index):
                    for ii in range(last_layer_from_index, last_layer_to_index):
                        if random.random() < connect_prob:
                            edges.append([ii, i])
                last_layer_from_index = this_layer_from_index
                last_layer_to_index = this_layer_to_index
            # connect all orphans to root
            orphans = set(range(len(ranks))) - set([edge[1] for edge in edges])
            for orphan in orphans:
                if orphan!=0:
                    edges.append([0,orphan])
            # connect all leafs to sink
            leafs = set(range(len(ranks))) - set([edge[0] for edge in edges])
            for leaf in leafs:
                edges.append([leaf,len(ranks)])
            ranks.append(layer_num+1) # add sink node
            # generate task properties from DAG
            n_tasks = len(ranks)
            preds = lambda dst_id: tuple(edge[0] for edge in edges if edge[1] == dst_id)
            succs = lambda src_id: tuple(edge[1] for edge in edges if edge[0] == src_id)
            # Assume that tasks with the same rank and the same successors are of the same category
            features=[(ranks[i],succs(i)) for i in range(n_tasks)]
            deduplicated_features=list(set(features))
            n_categories = len(deduplicated_features)
            categories=[deduplicated_features.index(feature) for feature in features]
            # Assume that tasks of the same category have the same workload
            workload_of_category=[random.uniform(min_workload,max_workload) for _ in range(n_categories)]
            # workloads[task_index] = workload of the task
            workloads=[workload_of_category[categories[i]] for i in range(n_tasks)]
            # write to markdown file
            f.write(f"### {save_name}_{workflow_id}\n")
            f.write("```mermaid\ngraph TD;\n")
            for edge in edges:
                src=f"T{edge[0]}R{ranks[edge[0]]}C{categories[edge[0]]}[{workloads[edge[0]]:.2f}]"
                dst=f"T{edge[1]}R{ranks[edge[1]]}C{categories[edge[1]]}[{workloads[edge[1]]:.2f}]"
                f.write(f" {src}-->{dst};\n")
            f.write("```\n\n\n")
    print(path_save)


def load_workflows(load_name="edge_workflows"):
    workflows=[]
    with open(f"{path_this}/{load_name}.md","r",encoding='utf-8') as f:
        markdown = f.read()
    for code_block in re.findall("```mermaid\ngraph TD;\n([^`]*)```",markdown):
        tasks={}
        dataflows=[]
        for ret in re.findall(" T(\d+)R(\d+)C(\d+)\[([0-9\.]+)\]-->T(\d+)R(\d+)C(\d+)\[([0-9\.]+)\];",code_block):
            src={'id':int(ret[0]),'rank':int(ret[1]),'category':int(ret[2]),'workload':float(ret[3])}
            dst={'id':int(ret[4]),'rank':int(ret[5]),'category':int(ret[6]),'workload':float(ret[7])}
            tasks.setdefault(src['id'],src)
            tasks.setdefault(dst['id'],dst)
            dataflows.append({'src':src['id'],'dst':dst['id'],'datasize':round((src['workload']+dst['workload'])/2,2)})
        workflows.append({
            'id':len(workflows),
            'tasks':sorted(tasks.values(),key=lambda x:x['id']),
            'dataflows':dataflows
        })
    return workflows
    
if __name__=='__main__':
    generate_random_graph()
    workflows = load_workflows()
    print(workflows[0])
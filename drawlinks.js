var nodesHash = {};
var nodes = [];
var links = [];

var width = screen.width,
	height = screen.height;

var color = d3.scale.category20();

var force = d3.layout.force()
	.charge(-120)
	.linkDistance(30)
	.size([width, height])
	// .linkStrength(0.9)
	// .friction(0.9)
	// .gravity(0.05)

var svg = d3.select("body").append("svg")
	.attr("width", width)
	.attr("height", height);

force.nodes(nodes)
	.links(links)
	.start();

var link = svg.selectAll(".link")
	.data(links)
	.enter().append("line")
	.attr("class", "link")
	.style("stroke-width", function(d) { return Math.sqrt(d.value); });

var node = svg.selectAll(".node")
	.data(nodes)
	.enter().append("circle")
	.attr("class", "node")
	.attr("r", 5)
	.style("fill", function(d) { return color(d.group); })
	.call(force.drag);

node.append("title")
	.text(function(d) { return d.name; });

force.on("tick", function() {
link.attr("x1", function(d) { return d.source.x; })
	.attr("y1", function(d) { return d.source.y; })
	.attr("x2", function(d) { return d.target.x; })
	.attr("y2", function(d) { return d.target.y; });

node.attr("cx", function(d) { return d.x; })
	.attr("cy", function(d) { return d.y; });
});

var REDRAW_INTERVAL = 500;
var jobid = setInterval(updateDraw, REDRAW_INTERVAL);
// d3.select('svg').on('click', function(){
// 	clearInterval(jobid);
// 	d3.select('svg').remove();
// });

var inDrawState;
var inDrawStateTime = 0;
function updateDraw(){
	inDrawStateTime++;
	// console.log('inDrawState:'+inDrawState);
	if(inDrawState){
		if(inDrawStateTime > 4){ inDrawState = undefined; }
		return;
	}
	inDrawState = true;
	inDrawStateTime = 0;
	d3.json('getstat', function(err, data){
		if(err){
			return;
		}
		links = [];
		gossipstat = data;
		nodesHash = {};

		// reset counted values (in/out/role)
		for(var idx in nodes){
			var nd = nodes[idx];
			nd.linksin = 0;
			nd.linksout = 0;
			nd.role = '';
			nd.actual = false;
			nodesHash[nd.name] = +idx;
		}

		var checkPeer = function(peer){
			if(nodesHash[peer] === undefined){
				nodesHash[peer] = nodes.push({
					name : peer,
					linksin : 0,
					linksout : 0,
					role : '',
					color : undefined
				}) - 1;
			}
		};

		var getNodeByPeer = function(peer){
			return nodes[nodesHash[peer]];
		}

		for(var i = 0; i < data.length; i++){
			var info = data[i];

			if(info.type == 'N'){
				var peer = info.host;
				checkPeer(peer);
				var nd = getNodeByPeer(peer)
				nd.color = info.color;
				nd.role = info.role;
				nd.actual = true;
			}
			else if(info.type == 'L'){
				var peer_host = info.host;
				var peer_from = info.link;
				checkPeer(peer_host);
				checkPeer(peer_from);

				if(info.ctype == 'in'){
					nodes[nodesHash[peer_host]].linksin++;
				}
				else if(info.ctype == 'out'){
					nodes[nodesHash[peer_from]].linksout++;
				}

				links.push({
					source : nodesHash[peer_host],
					target : nodesHash[peer_from]
				});
			}
		}

		// DRAW LINKS
		link = link.data(links);
		link.enter().insert("line", ".node")
			.attr("class", "link")
		link.exit().remove();

		// REDRAW NODES
		node = node.data(nodes);

		node.enter().insert("circle")
			.attr("class", "node")
			.on('mouseover', function(d){
				var shift = window.event.shiftKey;
				if(shift){
					console.log('resetting '+ d.name);
					d3.text('reset?node=' + d.name, function(err){
						if(err){ console.log(err); }
					});
					return;
				}
				console.log(d.name)
			})
			.attr("r", 5)
		.transition()
			.duration(500)
			.ease("elastic")

		node.append("title")
  			.text(function(d) {
  				return d && d.name;
  			})

		node.exit()
			.transition()
			.attr("r", 0)
			.remove();

		// update values
		svg.selectAll('circle')
			.style("fill", function(d) {
				// if(!d.actual) { return 'lightgray'; }
				if(d.role === 'N'){
					return 'red';
				}
				else if(d.role === 'S'){
					return 'violet';
				}
				return d.color || 'steelblue';
			})
			.attr('r', function(d){
				return d.actual ? 5 : 3;
			});

		force
			.nodes(nodes)
			.links(links)
			.start();

		inDrawState = false;
	});
}

window.onload = function(){
	var cmd = document.getElementById('cmd');
	cmd.addEventListener('change', function(){
		var color = cmd.value;
		d3.text('cmd?color=' + color, function(err){
			if(err){ console.log(err); }
		});
	});
}






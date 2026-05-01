// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  site: "https://bigfleet.lucy.sh",
  integrations: [
    starlight({
      title: "BigFleet",
      description:
        "A fleet-level infrastructure autoscaler. Many clusters, one fleet, one decision engine.",
      logo: { src: "./src/assets/logo.svg", replacesTitle: false },
      social: {
        github: "https://github.com/intUnderflow/bigfleet",
      },
      editLink: {
        baseUrl:
          "https://github.com/intUnderflow/bigfleet/edit/main/site/src/content/docs/",
      },
      sidebar: [
        {
          label: "Start here",
          items: [
            { label: "Overview", link: "/" },
            { label: "User stories", link: "/user-stories/" },
            { label: "Quickstart", link: "/quickstart/" },
            { label: "Concepts", link: "/concepts/" },
          ],
        },
        {
          label: "Architecture",
          items: [
            { label: "Architecture overview", link: "/architecture/" },
            { label: "API reference", link: "/api-reference/" },
          ],
        },
        {
          label: "Operating",
          items: [
            { label: "Operator guide", link: "/operator-guide/" },
            { label: "Scaling guide", link: "/scaling-guide/" },
          ],
        },
        {
          label: "Extending",
          items: [
            { label: "Provider author guide", link: "/provider-author-guide/" },
          ],
        },
        {
          label: "Background",
          items: [
            { label: "Implementation plan", link: "/plan/" },
            {
              label: "BigFleet paper",
              link: "/papers/bigfleet/",
            },
            {
              label: "Fleet-Scale Kubernetes paper",
              link: "/papers/fleet-scale-kubernetes/",
            },
            { label: "ADR index", link: "/adr/" },
          ],
        },
      ],
      customCss: ["./src/styles/custom.css"],
    }),
  ],
});
